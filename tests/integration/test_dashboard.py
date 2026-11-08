"""Static dashboard built from the marts."""

import json
import re

import pytest

from openneuro_pipeline.dashboard import build
from openneuro_pipeline.dashboard.build import render
from openneuro_pipeline.errors import StorageError
from openneuro_pipeline.extract import ExtractParams


def embedded_data(html: str) -> dict:
    match = re.search(
        r'<script id="dashboard-data" type="application/json">(.*?)</script>', html, re.S
    )
    return json.loads(match.group(1))


@pytest.fixture
def built_site(pipeline, mocked_api, tmp_path):
    pipeline.run(ExtractParams())
    docs = pipeline.dbt.generate_docs()
    return build(pipeline.settings, tmp_path / "site", docs_dir=docs)


def test_page_embeds_mart_numbers(built_site):
    html = (built_site.output_dir / "index.html").read_text()
    data = embedded_data(html)

    assert built_site.n_datasets == 4
    assert data["summary"]["n_datasets"] == 4
    assert data["summary"]["n_participants"] == 4
    assert {row["attribute"] for row in data["standardisation"]} == {"age", "sex", "handedness"}
    assert data["datasets"][0]["dataset_id"] == "ds000001"  # sorted by subjects, then id
    assert (
        json.loads((built_site.output_dir / "data.json").read_text())["summary"] == data["summary"]
    )


def test_quality_panel_uses_latest_dbt_results(built_site):
    quality = embedded_data((built_site.output_dir / "index.html").read_text())["quality"]
    assert quality["n_tests"] >= 40
    assert quality["warnings"] == []


def test_dbt_docs_are_bundled_and_linked(built_site):
    docs = built_site.output_dir / "dbt-docs"
    assert {p.name for p in docs.iterdir()} >= {"index.html", "manifest.json", "catalog.json"}
    data = embedded_data((built_site.output_dir / "index.html").read_text())
    assert data["docs_url"] == "dbt-docs/"


def test_repo_link_comes_from_github_environment(pipeline, mocked_api, tmp_path, monkeypatch):
    monkeypatch.delenv("DASHBOARD_REPO_URL", raising=False)
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/repo")
    pipeline.run(ExtractParams(), transform=True)
    result = build(pipeline.settings, tmp_path / "site")
    data = embedded_data((result.output_dir / "index.html").read_text())
    assert data["repo_url"] == "https://github.com/me/repo"


def test_text_cannot_break_out_of_the_data_block():
    html = render({"datasets": [{"dataset_name": "</script><script>alert(1)</script>"}]})
    # The only closing tags are the page's own two scripts.
    assert html.count("</script>") == 2
    assert embedded_data(html)["datasets"][0]["dataset_name"].startswith("</script>")


def test_missing_warehouse_is_a_clear_error(settings):
    with pytest.raises(StorageError, match="make run"):
        build(settings)

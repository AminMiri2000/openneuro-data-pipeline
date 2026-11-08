import json

import pytest
from click.testing import CliRunner

from openneuro_pipeline.cli import EXIT_ERROR, main
from tests.conftest import GRAPHQL_URL, S3_URL


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("OPENNEURO_GRAPHQL_URL", GRAPHQL_URL)
    monkeypatch.setenv("OPENNEURO_S3_BASE_URL", S3_URL)
    monkeypatch.setenv("OPENNEURO_PAGE_SIZE", "2")
    return tmp_path / "data"


def _json_output(output: str):
    return json.loads(output[output.index("{") :])


def test_run_end_to_end(env, mocked_api):
    result = CliRunner().invoke(main, ["run", "--ingest-date", "2026-11-07"])

    assert result.exit_code == 0, result.output
    summary = _json_output(result.output)
    datasets, participants = summary["extracts"]
    assert datasets["metrics"]["records"] == 4
    assert participants["metrics"]["fetched"] == 2
    assert summary["load"]["tables"]["raw.openneuro_participants"]["rows_inserted"] == 4
    assert (env / "warehouse.duckdb").exists()


def test_extract_only_participants_pulls_in_datasets(env, mocked_api):
    result = CliRunner().invoke(main, ["extract", "participants"])
    assert result.exit_code == 0, result.output
    assert [r["step"] for r in json.loads(result.output[result.output.index("[") :])] == [
        "datasets",
        "participants",
    ]


def test_config_error_gives_clean_exit(env, monkeypatch):
    monkeypatch.setenv("PIPELINE_MAX_WORKERS", "0")
    result = CliRunner().invoke(main, ["load"])
    assert result.exit_code == EXIT_ERROR

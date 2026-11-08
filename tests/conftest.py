"""Shared fixtures. All HTTP is mocked: the suite never touches the network."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import responses

from openneuro_pipeline.config import Settings
from openneuro_pipeline.pipeline import Pipeline
from openneuro_pipeline.sources.openneuro import OpenNeuroClient
from openneuro_pipeline.storage import LocalObjectStore

FIXTURES = Path(__file__).parent / "fixtures"
DBT_PROJECT_DIR = Path(__file__).parent.parent / "transform"
GRAPHQL_URL = "https://openneuro.test/crn/graphql"
S3_URL = "https://s3.test/openneuro.org"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        graphql_url=GRAPHQL_URL,
        s3_base_url=S3_URL,
        data_dir=tmp_path / "data",
        page_size=2,
        request_timeout=5,
        max_workers=2,
        insert_batch_size=2,  # tiny, so batching is exercised by small fixtures
        dbt_project_dir=DBT_PROJECT_DIR,
    )


@pytest.fixture
def client(settings: Settings) -> OpenNeuroClient:
    # No retries: tests that simulate failures should fail immediately.
    return OpenNeuroClient(settings, max_retries=0)


@pytest.fixture
def store(settings: Settings) -> LocalObjectStore:
    return LocalObjectStore(settings.raw_dir)


@pytest.fixture
def pipeline(settings: Settings, client: OpenNeuroClient, store: LocalObjectStore) -> Pipeline:
    return Pipeline(settings, client=client, store=store)


def register_catalogue(rsps: responses.RequestsMock) -> None:
    """Two catalogue pages; page 2 has a null node, a partial error and an unpublished dataset."""
    page1, page2 = load_fixture("datasets_page1.json"), load_fixture("datasets_page2.json")

    def callback(request: Any) -> tuple[int, dict[str, str], str]:
        after = json.loads(request.body)["variables"].get("after")
        return 200, {}, json.dumps(page2 if after == "cursor-2" else page1)

    rsps.add_callback(responses.POST, GRAPHQL_URL, callback=callback)


def register_participant_files(rsps: responses.RequestsMock) -> None:
    """ds000001: TSV + sidecar; ds000002: TSV only; ds000003: no participant files."""
    rsps.add(
        responses.GET,
        f"{S3_URL}/ds000001/participants.tsv",
        body=(FIXTURES / "ds000001_participants.tsv").read_bytes(),
    )
    rsps.add(
        responses.GET,
        f"{S3_URL}/ds000001/participants.json",
        body=(FIXTURES / "ds000001_participants.json").read_bytes(),
    )
    rsps.add(
        responses.GET,
        f"{S3_URL}/ds000002/participants.tsv",
        body=b"participant_id\tage\nsub-01\t30\n",
    )
    rsps.add(responses.GET, f"{S3_URL}/ds000002/participants.json", status=403)
    rsps.add(responses.GET, f"{S3_URL}/ds000003/participants.tsv", status=404)
    rsps.add(responses.GET, f"{S3_URL}/ds000003/participants.json", status=404)


@pytest.fixture
def mocked_api() -> Iterator[responses.RequestsMock]:
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        register_catalogue(rsps)
        register_participant_files(rsps)
        yield rsps


def json_from_output(output: str) -> Any:
    """Extract the JSON summary the CLI prints last, ignoring log lines before it.

    The summary is the only text that starts at column 0 with "{" or "[".
    """
    lines = output.splitlines()
    start = next(i for i, line in enumerate(lines) if line in ("{", "["))
    return json.loads("\n".join(lines[start:]))

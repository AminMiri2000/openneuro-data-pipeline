"""Shared fixtures. All HTTP is mocked: the test suite never touches the network."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import responses

from openneuro_pipeline.client import OpenNeuroClient
from openneuro_pipeline.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"
GRAPHQL_URL = "https://openneuro.test/crn/graphql"
S3_URL = "https://s3.test/openneuro.org"


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        graphql_url=GRAPHQL_URL,
        s3_base_url=S3_URL,
        data_dir=tmp_path / "data",
        page_size=2,
        request_timeout=5,
    )


@pytest.fixture
def client(settings: Settings) -> OpenNeuroClient:
    return OpenNeuroClient(settings)


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def mocked_api():
    """Two pages of datasets, plus S3 files for ds000001 (full), ds000002 (TSV only)."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        page1 = load_fixture("datasets_page1.json")
        page2 = load_fixture("datasets_page2.json")

        def graphql_callback(request):
            body = json.loads(request.body)
            after = body["variables"].get("after")
            return (200, {}, json.dumps(page2 if after == "cursor-2" else page1))

        rsps.add_callback(responses.POST, GRAPHQL_URL, callback=graphql_callback)
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
        yield rsps

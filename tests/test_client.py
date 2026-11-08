import json

import pytest
import responses

from openneuro_pipeline.client import OpenNeuroError, PageStats
from tests.conftest import GRAPHQL_URL


def test_paginates_and_skips_null_nodes(client, mocked_api):
    stats = PageStats()
    ids = [n["id"] for n in client.iter_datasets(modality="EEG", stats=stats)]

    assert ids == ["ds000001", "ds000002", "ds000003", "ds000004"]
    assert stats.pages == 2
    assert stats.null_nodes == 1
    assert stats.graphql_errors == ["Snapshot could not be resolved"]


def test_limit_stops_early(client, mocked_api):
    ids = [n["id"] for n in client.iter_datasets(limit=1)]
    assert ids == ["ds000001"]


def test_sends_cursor_on_second_page(client, mocked_api):
    list(client.iter_datasets(modality="EEG"))
    sent = [json.loads(c.request.body)["variables"] for c in mocked_api.calls]
    assert sent[0]["after"] is None and sent[0]["modality"] == "EEG"
    assert sent[1]["after"] == "cursor-2"


@responses.activate
def test_errors_without_data_raise(client):
    responses.add(responses.POST, GRAPHQL_URL, json={"errors": [{"message": "bad field"}]})
    with pytest.raises(OpenNeuroError, match="bad field"):
        list(client.iter_datasets())


@responses.activate
def test_stuck_cursor_raises(client):
    page = {
        "data": {"datasets": {"edges": [], "pageInfo": {"hasNextPage": True, "endCursor": None}}}
    }
    responses.add(responses.POST, GRAPHQL_URL, json=page)
    with pytest.raises(OpenNeuroError, match="cursor"):
        list(client.iter_datasets())


@responses.activate
def test_missing_s3_file_returns_none(client, settings):
    responses.add(responses.GET, f"{settings.s3_base_url}/ds9/participants.tsv", status=404)
    assert client.fetch_dataset_file("ds9", "participants.tsv") is None

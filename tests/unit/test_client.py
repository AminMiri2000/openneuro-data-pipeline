import json
import threading

import pytest
import responses

from openneuro_pipeline.errors import SourceError
from openneuro_pipeline.sources.openneuro import PageStats
from tests.conftest import GRAPHQL_URL


def test_paginates_and_skips_null_nodes(client, mocked_api):
    stats = PageStats()
    ids = [node["id"] for node in client.iter_datasets(modality="EEG", stats=stats)]

    assert ids == ["ds000001", "ds000002", "ds000003", "ds000004"]
    assert (stats.pages, stats.nodes, stats.null_nodes) == (2, 4, 1)
    assert stats.graphql_errors == ["Snapshot could not be resolved"]


def test_limit_stops_early(client, mocked_api):
    assert [n["id"] for n in client.iter_datasets(limit=1)] == ["ds000001"]
    assert len(mocked_api.calls) == 1  # second page never requested


def test_sends_cursor_on_second_page(client, mocked_api):
    list(client.iter_datasets(modality="EEG"))
    sent = [json.loads(call.request.body)["variables"] for call in mocked_api.calls]
    assert sent[0]["after"] is None and sent[0]["modality"] == "EEG"
    assert sent[1]["after"] == "cursor-2"


@responses.activate
def test_errors_without_data_raise(client):
    responses.add(responses.POST, GRAPHQL_URL, json={"errors": [{"message": "bad field"}]})
    with pytest.raises(SourceError, match="bad field"):
        list(client.iter_datasets())


@responses.activate
def test_stuck_cursor_raises(client):
    page = {
        "data": {"datasets": {"edges": [], "pageInfo": {"hasNextPage": True, "endCursor": None}}}
    }
    responses.add(responses.POST, GRAPHQL_URL, json=page)
    with pytest.raises(SourceError, match="cursor"):
        list(client.iter_datasets())


@responses.activate
def test_missing_s3_file_returns_none(client, settings):
    responses.add(responses.GET, f"{settings.s3_base_url}/ds9/participants.tsv", status=404)
    assert client.fetch_dataset_file("ds9", "participants.tsv") is None


def test_each_thread_gets_its_own_session(client):
    sessions = []
    worker = threading.Thread(target=lambda: sessions.append(client.session))
    worker.start()
    worker.join()
    assert client.session is client.session  # stable within a thread
    assert sessions[0] is not client.session  # distinct across threads

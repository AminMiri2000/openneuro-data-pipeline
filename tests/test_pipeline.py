"""End-to-end behaviour of ingest -> load against mocked OpenNeuro."""

from datetime import date

import duckdb
import responses

from openneuro_pipeline.ingest.datasets import ingest_datasets
from openneuro_pipeline.ingest.participants import ingest_participants
from openneuro_pipeline.load import load_raw
from openneuro_pipeline.storage import StateStore, read_jsonl_gz
from tests.conftest import S3_URL

DAY = date(2026, 11, 7)


def test_datasets_rerun_same_day_is_idempotent(client, settings, mocked_api):
    first = ingest_datasets(client, settings, "EEG", ingest_date=DAY)
    second = ingest_datasets(client, settings, "EEG", ingest_date=DAY)

    assert first.path == second.path
    assert first.records == second.records == 4
    files = list((settings.raw_dir / "datasets").rglob("*.jsonl.gz"))
    assert len(files) == 1  # overwritten, not duplicated
    rec = read_jsonl_gz(files[0])[0]
    assert rec["dataset"]["id"] == "ds000001" and rec["_modality_filter"] == "EEG"


def test_participants_are_incremental(client, settings, mocked_api):
    ingest_datasets(client, settings, "EEG", ingest_date=DAY)

    first = ingest_participants(client, settings, "EEG")
    # ds000004 has no snapshot -> not a candidate; ds000003 has no TSV -> "missing"
    assert (first.candidates, first.fetched, first.missing, first.failed) == (3, 2, 1, [])

    calls_before = len(mocked_api.calls)
    second = ingest_participants(client, settings, "EEG")
    assert second.skipped_unchanged == 3 and second.fetched == 0
    assert len(mocked_api.calls) == calls_before  # nothing re-downloaded


def test_new_snapshot_triggers_refetch(client, settings, mocked_api):
    ingest_datasets(client, settings, "EEG", ingest_date=DAY)
    ingest_participants(client, settings, "EEG")

    # Simulate ds000002 publishing a new snapshot the next day.
    path = next((settings.raw_dir / "datasets").rglob("*.jsonl.gz"))
    records = read_jsonl_gz(path)
    for r in records:
        if r["dataset"]["id"] == "ds000002":
            r["dataset"]["latestSnapshot"]["tag"] = "2.0.0"
    newer = settings.raw_dir / "datasets" / "ingest_date=2026-11-08" / "modality=eeg"
    from openneuro_pipeline.storage import write_jsonl_gz

    write_jsonl_gz(newer / "datasets.jsonl.gz", records)

    result = ingest_participants(client, settings, "EEG")
    assert result.fetched == 1 and result.skipped_unchanged == 2


def test_failed_dataset_is_retried_next_run(client, settings):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        from tests.conftest import GRAPHQL_URL, load_fixture

        rsps.add(
            responses.POST,
            GRAPHQL_URL,
            json={
                "data": {
                    "datasets": {
                        "edges": load_fixture("datasets_page1.json")["data"]["datasets"]["edges"][
                            :1
                        ],
                        "pageInfo": {"hasNextPage": False, "endCursor": "c"},
                    }
                }
            },
        )
        rsps.add(responses.GET, f"{S3_URL}/ds000001/participants.tsv", status=500)
        ingest_datasets(client, settings, "EEG", ingest_date=DAY)
        # Keep the test fast: no retry back-off.
        client.session.adapters["https://"].max_retries.total = 0
        failed = ingest_participants(client, settings, "EEG")
        assert len(failed.failed) == 1
        state = StateStore(settings.state_dir / "participants.json").load()
        assert "ds000001" not in state.get("participants_watermark", {})  # not advanced

        # Next run, S3 is healthy again: the dataset is picked up automatically.
        rsps.replace(
            responses.GET, f"{S3_URL}/ds000001/participants.tsv", body=b"participant_id\nsub-01\n"
        )
        rsps.add(responses.GET, f"{S3_URL}/ds000001/participants.json", status=404)
        retried = ingest_participants(client, settings, "EEG")
    assert retried.fetched == 1 and retried.failed == []


def test_load_builds_raw_tables(client, settings, mocked_api):
    ingest_datasets(client, settings, "EEG", ingest_date=DAY)
    ingest_participants(client, settings, "EEG")

    first = load_raw(settings)
    second = load_raw(settings)  # rebuild is idempotent
    assert first == second
    assert first.datasets_rows == 4
    assert first.participants_rows == 4  # 3 in ds000001 + 1 in ds000002
    assert first.participant_columns_rows == 3

    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    row = con.execute("""
        SELECT ingest_date, modality_filter, payload -> '$.latestSnapshot' ->> '$.tag'
        FROM raw.openneuro_datasets WHERE dataset_id = 'ds000001'
    """).fetchone()
    assert row == (DAY, "eeg", "1.0.2")

    nulls = con.execute("""
        SELECT attributes ->> '$.age' FROM raw.openneuro_participants
        WHERE dataset_id = 'ds000001' AND participant_id = 'sub-02'
    """).fetchone()
    assert nulls == (None,)  # BIDS 'n/a' becomes a real NULL

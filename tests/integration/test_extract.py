"""Extractors end to end against mocked OpenNeuro and a temporary raw layer."""

from datetime import date

import pytest
import responses

from openneuro_pipeline.errors import StorageError
from openneuro_pipeline.extract import ExtractParams, ExtractResult, RunContext, get_extractor
from openneuro_pipeline.storage import WatermarkStore, layout
from openneuro_pipeline.storage.formats import iter_jsonl_gz, write_jsonl_gz
from tests.conftest import S3_URL, register_catalogue

DAY = date(2026, 11, 7)
PARAMS = ExtractParams(modality="EEG", ingest_date=DAY)


def run_alone(pipeline, name: str) -> ExtractResult:
    """Run one extractor without its dependencies (Pipeline.extract adds them)."""
    context = RunContext.create(pipeline.settings, pipeline.client, pipeline.store)
    return get_extractor(name)(context).run(PARAMS)


def test_datasets_rerun_same_day_is_idempotent(pipeline, store, mocked_api):
    first = pipeline.extract(["datasets"], PARAMS)[0]
    second = pipeline.extract(["datasets"], PARAMS)[0]

    assert first.metrics["key"] == second.metrics["key"]
    assert first.metrics["records"] == 4
    assert first.ok and first.warnings == ["graphql: Snapshot could not be resolved"]
    assert len(list(store.list_keys(layout.datasets_prefix()))) == 1  # overwritten, not duplicated


def test_runs_write_manifests(pipeline, store, mocked_api):
    pipeline.extract(params=PARAMS)
    manifests = list(store.list_keys(f"{layout.SYSTEM_PREFIX}/runs"))
    assert len(manifests) == 2
    assert {key.split("/")[2] for key in manifests} == {"datasets", "participants"}


def test_participants_are_incremental(pipeline, mocked_api):
    _, first = pipeline.extract(params=PARAMS)
    # ds000004 has no snapshot (not a candidate); ds000003 has no TSV ("missing").
    assert first.metrics == {"candidates": 3, "skipped_unchanged": 0, "fetched": 2, "missing": 1}

    calls_before = len(mocked_api.calls)
    second = run_alone(pipeline, "participants")
    assert second.metrics["skipped_unchanged"] == 3 and second.metrics["fetched"] == 0
    assert len(mocked_api.calls) == calls_before  # nothing re-downloaded


def test_new_snapshot_triggers_refetch(pipeline, store, mocked_api):
    pipeline.extract(params=PARAMS)

    # Simulate ds000002 publishing snapshot 2.0.0 in the next day's catalogue.
    records = list(iter_jsonl_gz(store, layout.datasets_key(DAY, "EEG")))
    for record in records:
        if record["dataset"]["id"] == "ds000002":
            record["dataset"]["latestSnapshot"]["tag"] = "2.0.0"
    write_jsonl_gz(store, layout.datasets_key(date(2026, 11, 8), "EEG"), records)

    result = run_alone(pipeline, "participants")
    assert result.metrics["fetched"] == 1 and result.metrics["skipped_unchanged"] == 2


def test_failed_dataset_is_retried_next_run(pipeline, store):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        register_catalogue(rsps)
        for ds in ("ds000002", "ds000003"):
            rsps.add(responses.GET, f"{S3_URL}/{ds}/participants.tsv", status=404)
        rsps.add(responses.GET, f"{S3_URL}/ds000001/participants.tsv", status=500)

        failed = pipeline.extract(params=PARAMS)[-1]
        assert not failed.ok and failed.errors[0].startswith("ds000001@1.0.2")
        assert "ds000001" not in WatermarkStore(store, "participants_watermark")

        # S3 recovers: the next run picks the dataset up automatically.
        rsps.replace(
            responses.GET, f"{S3_URL}/ds000001/participants.tsv", body=b"participant_id\nsub-01\n"
        )
        rsps.add(responses.GET, f"{S3_URL}/ds000001/participants.json", status=404)
        retried = run_alone(pipeline, "participants")

    assert retried.ok and retried.metrics["fetched"] == 1


def test_participants_without_catalogue_fails_clearly(pipeline):
    with pytest.raises(StorageError, match="run the 'datasets' extractor first"):
        run_alone(pipeline, "participants")

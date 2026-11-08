"""Incremental warehouse loading."""

from datetime import date

import pytest

from openneuro_pipeline.extract import ExtractParams
from openneuro_pipeline.storage import layout
from openneuro_pipeline.storage.formats import iter_jsonl_gz, write_jsonl_gz

DAY = date(2026, 11, 7)
PARAMS = ExtractParams(modality="EEG", ingest_date=DAY)


@pytest.fixture
def extracted(pipeline, mocked_api):
    pipeline.extract(params=PARAMS)
    return pipeline


def query(pipeline, sql):
    with pipeline.warehouse.connect(read_only=True) as con:
        return con.execute(sql).fetchall()


def test_first_load_inserts_everything(extracted):
    result = extracted.load()
    tables = result.tables

    assert tables["raw.openneuro_datasets"].rows_inserted == 4
    assert tables["raw.openneuro_participants"].rows_inserted == 4  # 3 + 1, across 2 batches
    assert tables["raw.openneuro_participant_columns"].rows_inserted == 3

    row = query(
        extracted,
        "SELECT ingest_date, modality_filter, payload->'latestSnapshot'->>'tag' "
        "FROM raw.openneuro_datasets WHERE dataset_id = 'ds000001'",
    )
    assert row == [(DAY, "eeg", "1.0.2")]
    age = query(
        extracted,
        "SELECT attributes->>'age' FROM raw.openneuro_participants "
        "WHERE dataset_id = 'ds000001' AND participant_id = 'sub-02'",
    )
    assert age == [(None,)]  # BIDS 'n/a' became a real NULL


def test_second_load_skips_unchanged_partitions(extracted):
    extracted.load()
    again = extracted.load()

    for stats in again.tables.values():
        assert stats.loaded_partitions == 0 and stats.rows_inserted == 0
    assert query(extracted, "SELECT count(*) FROM raw.openneuro_datasets") == [(4,)]


def test_only_new_partitions_are_loaded(extracted, store):
    extracted.load()
    next_day = layout.datasets_key(date(2026, 11, 8), "EEG")
    write_jsonl_gz(store, next_day, iter_jsonl_gz(store, layout.datasets_key(DAY, "EEG")))

    result = extracted.load()
    datasets = result.tables["raw.openneuro_datasets"]
    assert (datasets.loaded_partitions, datasets.skipped_partitions) == (1, 1)
    assert query(extracted, "SELECT count(DISTINCT ingest_date) FROM raw.openneuro_datasets") == [
        (2,)
    ]


def test_rewritten_partition_is_replaced_not_duplicated(extracted, store):
    extracted.load()
    key = layout.datasets_key(DAY, "EEG")
    records = list(iter_jsonl_gz(store, key))[:2]  # same day re-extracted, fewer rows
    write_jsonl_gz(store, key, records)

    extracted.load()
    assert query(extracted, "SELECT count(*) FROM raw.openneuro_datasets") == [(2,)]


def test_full_refresh_rebuilds(extracted):
    extracted.load()
    result = extracted.load(full_refresh=True)
    assert result.tables["raw.openneuro_datasets"].loaded_partitions == 1
    assert query(extracted, "SELECT count(*) FROM raw._load_log") == [(5,)]

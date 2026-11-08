from datetime import date

import pytest

from openneuro_pipeline.errors import StorageError
from openneuro_pipeline.storage import LocalObjectStore, ObjectStore, WatermarkStore, layout
from openneuro_pipeline.storage.formats import iter_jsonl_gz, write_jsonl_gz


@pytest.fixture
def store(tmp_path):
    return LocalObjectStore(tmp_path)


def test_local_store_satisfies_protocol(store):
    assert isinstance(store, ObjectStore)


def test_write_read_list_stat(store):
    store.write_bytes("a/b/one.txt", b"1")
    store.write_bytes("a/two.txt", b"22")

    assert store.read_bytes("a/b/one.txt") == b"1"
    assert list(store.list_keys("a")) == ["a/b/one.txt", "a/two.txt"]
    assert store.stat("a/two.txt").size == 2


def test_fingerprint_changes_on_rewrite(store):
    store.write_bytes("k", b"old")
    before = store.stat("k").fingerprint
    store.write_bytes("k", b"newer")
    assert store.stat("k").fingerprint != before


def test_failed_write_leaves_nothing_behind(store, tmp_path):
    with pytest.raises(RuntimeError), store.open_writer("x/file.bin") as fh:
        fh.write(b"partial")
        raise RuntimeError("crash mid-write")

    assert not store.exists("x/file.bin")
    assert list(store.list_keys()) == []  # no stray temp files either


def test_missing_object_raises(store):
    with pytest.raises(StorageError):
        store.read_bytes("nope")


def test_rejects_keys_outside_root(store):
    with pytest.raises(StorageError):
        store.write_bytes("../escape.txt", b"x")


def test_jsonl_round_trip_is_deterministic(store):
    records = [{"b": 2, "a": 1}, {"c": [1, 2]}]
    assert write_jsonl_gz(store, "f.jsonl.gz", records) == 2
    first = store.read_bytes("f.jsonl.gz")
    write_jsonl_gz(store, "f.jsonl.gz", records)
    assert store.read_bytes("f.jsonl.gz") == first  # byte-identical output
    assert list(iter_jsonl_gz(store, "f.jsonl.gz")) == records


def test_layout_keys_and_partition_parsing():
    key = layout.datasets_key(date(2026, 11, 7), "EEG")
    assert key == "openneuro/datasets/ingest_date=2026-11-07/modality=eeg/datasets.jsonl.gz"
    assert layout.parse_partitions(key) == {"ingest_date": "2026-11-07", "modality": "eeg"}
    assert layout.modality_key(None) == "all"


def test_watermark_persists_only_on_flush(store):
    watermark = WatermarkStore(store, "demo")
    watermark.set("ds1", {"snapshot_tag": "1.0.0"})
    assert "ds1" not in WatermarkStore(store, "demo")

    watermark.flush()
    assert WatermarkStore(store, "demo").get("ds1") == {"snapshot_tag": "1.0.0"}

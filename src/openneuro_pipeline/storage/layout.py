"""Single source of truth for where objects live in the raw layer.

Every key is built here, so path conventions cannot drift between the
extractors that write files and the loader that reads them.

Partitioned folders use Hive-style `name=value` segments. DuckDB, Spark, Athena
and BigQuery external tables all understand that convention, so the layout
works unchanged if the raw layer moves to cloud storage.
"""

from __future__ import annotations

from datetime import date

SOURCE = "openneuro"
SYSTEM_PREFIX = "_system"  # pipeline bookkeeping, kept apart from source data

DATASETS_FILE = "datasets.jsonl.gz"
PARTICIPANTS_META_FILE = "_meta.json"


def modality_key(modality: str | None) -> str:
    """Normalise a modality filter into a partition value ('EEG' -> 'eeg', None -> 'all')."""
    return (modality or "all").lower()


def parse_partitions(key: str) -> dict[str, str]:
    """Extract Hive partition values from a key.

    >>> parse_partitions("openneuro/datasets/ingest_date=2026-11-07/modality=eeg/x.gz")
    {'ingest_date': '2026-11-07', 'modality': 'eeg'}
    """
    parts = (segment.split("=", 1) for segment in key.split("/") if "=" in segment)
    return {name: value for name, value in parts}


# ---------------------------------------------------------------- datasets


def datasets_prefix() -> str:
    return f"{SOURCE}/datasets"


def datasets_key(ingest_date: date, modality: str | None) -> str:
    """One file per (day, modality filter): re-running a day overwrites it."""
    return (
        f"{datasets_prefix()}/ingest_date={ingest_date.isoformat()}"
        f"/modality={modality_key(modality)}/{DATASETS_FILE}"
    )


# ------------------------------------------------------------ participants


def participants_prefix() -> str:
    return f"{SOURCE}/participants"


def participants_folder(dataset_id: str, snapshot_tag: str) -> str:
    return f"{participants_prefix()}/dataset_id={dataset_id}/snapshot_tag={snapshot_tag}"


def participants_file_key(dataset_id: str, snapshot_tag: str, filename: str) -> str:
    return f"{participants_folder(dataset_id, snapshot_tag)}/{filename}"


# ------------------------------------------------------------------ system


def state_key(name: str) -> str:
    return f"{SYSTEM_PREFIX}/state/{name}.json"


def manifest_key(step: str, run_id: str) -> str:
    return f"{SYSTEM_PREFIX}/runs/{step}/{run_id}.json"

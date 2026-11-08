"""Ingest each dataset's BIDS participant table (participants.tsv + participants.json).

Load strategy: incremental by snapshot.
Fetching a file per dataset is the expensive part of the pipeline, so it only
runs for datasets whose latest snapshot tag changed since the last successful
fetch. The (dataset_id -> snapshot_tag) watermark lives in data/state/.

Failure handling: one dataset failing does not stop the others. Its watermark is
not advanced, so the next run retries it automatically. State is saved after
every dataset, so an interrupted run resumes where it stopped.

Note: the public S3 bucket holds the dataset's current files, which normally
match the latest snapshot. The tag recorded here is the snapshot the catalogue
reported at fetch time.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from ..client import OpenNeuroClient
from ..config import Settings
from ..storage import StateStore, atomic_write_bytes, read_jsonl_gz
from .datasets import _write_manifest

log = logging.getLogger(__name__)

FILES = ("participants.tsv", "participants.json")
STATE_KEY = "participants_watermark"


@dataclass
class ParticipantsIngestResult:
    run_id: str
    candidates: int
    fetched: int = 0
    missing: int = 0
    skipped_unchanged: int = 0
    failed: list[str] = field(default_factory=list)


def latest_datasets_partition(settings: Settings, modality: str | None) -> Path | None:
    modality_key = (modality or "all").lower()
    matches = sorted(
        (settings.raw_dir / "datasets").glob(
            f"ingest_date=*/modality={modality_key}/datasets.jsonl.gz"
        )
    )
    return matches[-1] if matches else None


def snapshot_targets(records: list[dict]) -> dict[str, str]:
    """dataset_id -> latest snapshot tag, skipping datasets with no published snapshot."""
    targets: dict[str, str] = {}
    for rec in records:
        ds = rec.get("dataset") or {}
        snap = ds.get("latestSnapshot") or {}
        if ds.get("id") and snap.get("tag"):
            targets[ds["id"]] = snap["tag"]
    return targets


def participant_dir(settings: Settings, dataset_id: str, tag: str) -> Path:
    return settings.raw_dir / "participants" / f"dataset_id={dataset_id}" / f"snapshot_tag={tag}"


def ingest_participants(
    client: OpenNeuroClient,
    settings: Settings,
    modality: str | None = "EEG",
    limit: int | None = None,
) -> ParticipantsIngestResult:
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    partition = latest_datasets_partition(settings, modality)
    if partition is None:
        raise RuntimeError("No datasets partition found; run `ingest datasets` first.")

    targets = snapshot_targets(read_jsonl_gz(partition))
    store = StateStore(settings.state_dir / "participants.json")
    state = store.load()
    watermark: dict[str, dict] = state.setdefault(STATE_KEY, {})

    todo = [
        (ds, tag)
        for ds, tag in sorted(targets.items())
        if (watermark.get(ds) or {}).get("snapshot_tag") != tag
    ]
    result = ParticipantsIngestResult(
        run_id=run_id,
        candidates=len(targets),
        skipped_unchanged=len(targets) - len(todo),
    )
    if limit is not None:
        todo = todo[:limit]

    for dataset_id, tag in todo:
        try:
            found = _fetch_one(client, settings, dataset_id, tag, run_id)
        except Exception as exc:  # keep going; the watermark stays put so it is retried
            log.warning("participants: %s@%s failed: %s", dataset_id, tag, exc)
            result.failed.append(f"{dataset_id}@{tag}: {exc}")
            continue

        if found:
            result.fetched += 1
        else:
            result.missing += 1
        watermark[dataset_id] = {
            "snapshot_tag": tag,
            "has_participants_tsv": found,
            "fetched_at": datetime.now(UTC).isoformat(),
        }
        store.save(state)  # checkpoint after every dataset

    _write_manifest(settings, "participants", run_id, asdict(result), result.failed)
    log.info(
        "participants: fetched=%s missing=%s unchanged=%s failed=%s",
        result.fetched,
        result.missing,
        result.skipped_unchanged,
        len(result.failed),
    )
    return result


def _fetch_one(
    client: OpenNeuroClient, settings: Settings, dataset_id: str, tag: str, run_id: str
) -> bool:
    """Download the participant files for one dataset. Returns False if it has no TSV."""
    out_dir = participant_dir(settings, dataset_id, tag)
    contents = {name: client.fetch_dataset_file(dataset_id, name) for name in FILES}
    if contents["participants.tsv"] is None:
        return False

    for name, data in contents.items():
        if data is not None:
            atomic_write_bytes(out_dir / name, data)
    meta = {
        "dataset_id": dataset_id,
        "snapshot_tag": tag,
        "run_id": run_id,
        "ingested_at": datetime.now(UTC).isoformat(),
        "files": [n for n, d in contents.items() if d is not None],
    }
    atomic_write_bytes(out_dir / "_meta.json", json.dumps(meta, indent=2).encode())
    return True

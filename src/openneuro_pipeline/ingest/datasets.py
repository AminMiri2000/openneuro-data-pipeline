"""Ingest the OpenNeuro dataset catalogue (one record per dataset + its latest snapshot).

Load strategy: daily full snapshot.
The catalogue is small (a few thousand rows), so pulling it whole every run is
cheaper and simpler than change-tracking, and it lets the warehouse keep
history (one partition per day = a free "slowly changing" record of how each
dataset evolved).

Idempotency: the output path is determined by (ingest_date, modality filter),
so re-running on the same day overwrites the same partition instead of adding
duplicates. A run that fails part-way writes nothing (atomic rename).
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from ..client import OpenNeuroClient, PageStats
from ..config import Settings
from ..storage import atomic_write_bytes, write_jsonl_gz

log = logging.getLogger(__name__)

SOURCE = "openneuro_graphql.datasets"


@dataclass
class DatasetsIngestResult:
    run_id: str
    ingest_date: str
    modality: str
    path: str
    records: int
    pages: int
    null_nodes: int
    graphql_errors: int


def partition_path(settings: Settings, ingest_date: date, modality: str | None) -> Path:
    modality_key = (modality or "all").lower()
    return (
        settings.raw_dir
        / "datasets"
        / f"ingest_date={ingest_date.isoformat()}"
        / f"modality={modality_key}"
        / "datasets.jsonl.gz"
    )


def ingest_datasets(
    client: OpenNeuroClient,
    settings: Settings,
    modality: str | None = "EEG",
    limit: int | None = None,
    ingest_date: date | None = None,
) -> DatasetsIngestResult:
    ingested_at = datetime.now(UTC)
    ingest_date = ingest_date or ingested_at.date()
    run_id = f"{ingested_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    stats = PageStats()

    def records():
        for node in client.iter_datasets(modality=modality, limit=limit, stats=stats):
            yield {
                "_run_id": run_id,
                "_ingested_at": ingested_at.isoformat(),
                "_source": SOURCE,
                "_modality_filter": modality,
                "dataset": node,  # payload kept exactly as the API returned it
            }

    path = partition_path(settings, ingest_date, modality)
    count = write_jsonl_gz(path, records())

    result = DatasetsIngestResult(
        run_id=run_id,
        ingest_date=ingest_date.isoformat(),
        modality=(modality or "all").lower(),
        path=str(path),
        records=count,
        pages=stats.pages,
        null_nodes=stats.null_nodes,
        graphql_errors=len(stats.graphql_errors),
    )
    _write_manifest(settings, "datasets", result.run_id, asdict(result), stats.graphql_errors)
    log.info(
        "datasets: %s records over %s pages -> %s (null nodes: %s, graphql errors: %s)",
        count,
        stats.pages,
        path,
        stats.null_nodes,
        len(stats.graphql_errors),
    )
    return result


def _write_manifest(
    settings: Settings, step: str, run_id: str, summary: dict, errors: list[str]
) -> None:
    manifest = {**summary, "step": step, "errors_sample": errors[:20]}
    path = settings.raw_dir / "_runs" / f"{step}-{run_id}.json"
    atomic_write_bytes(path, json.dumps(manifest, indent=2).encode())

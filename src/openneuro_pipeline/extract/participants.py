"""Extract each dataset's BIDS participant table.

Strategy: incremental by snapshot. One download per dataset is the expensive
part of the pipeline, so it only runs for datasets whose latest snapshot tag
changed since the last successful fetch (tracked in a watermark).

Scaling: downloads are I/O-bound, so they run on a bounded thread pool
(`Settings.max_workers`). Only the coordinating thread touches the watermark,
and it checkpoints periodically, so an interrupted run resumes where it
stopped and a failed dataset is retried next time.

Note: the public S3 bucket serves a dataset's current files, which normally
match its latest snapshot. The tag stored is the one the catalogue reported.
"""

from __future__ import annotations

import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime

import requests

from ..errors import SourceError, StorageError
from ..storage import WatermarkStore, layout
from ..storage.formats import iter_jsonl_gz, write_json
from .base import Extractor, ExtractParams, ExtractResult
from .datasets import latest_partition_key
from .registry import register

log = logging.getLogger(__name__)

TSV_FILE = "participants.tsv"
SIDECAR_FILE = "participants.json"
WATERMARK_NAME = "participants_watermark"
CHECKPOINT_EVERY = 25  # completed datasets between watermark saves

# Failures we expect from the network or storage. Anything else is a bug and
# should stop the run loudly instead of being logged and skipped.
EXPECTED_ERRORS = (SourceError, StorageError, requests.RequestException, OSError)


@dataclass(frozen=True, order=True)
class SnapshotTarget:
    """A dataset at a specific published snapshot."""

    dataset_id: str
    snapshot_tag: str


@register
class ParticipantsExtractor(Extractor):
    """Downloads participant tables for datasets with a new snapshot."""

    name = "participants"
    depends_on = ("datasets",)

    def extract(self, params: ExtractParams) -> ExtractResult:
        targets = self._snapshot_targets(params.modality)
        watermark = WatermarkStore(self.ctx.store, WATERMARK_NAME)

        pending = [t for t in targets if not self._already_fetched(t, watermark)]
        skipped = len(targets) - len(pending)
        if params.limit is not None:
            pending = pending[: params.limit]

        fetched, missing, errors = self._fetch_all(pending, watermark)

        return ExtractResult(
            step=self.name,
            run_id=self.ctx.run_id,
            metrics={
                "candidates": len(targets),
                "skipped_unchanged": skipped,
                "fetched": fetched,
                "missing": missing,
            },
            errors=errors,
        )

    # ---------------------------------------------------------------- planning

    def _snapshot_targets(self, modality: str | None) -> list[SnapshotTarget]:
        """Read the newest catalogue partition and list datasets with a snapshot."""
        keys = list(self.ctx.store.list_keys(layout.datasets_prefix()))
        key = latest_partition_key(keys, modality)
        if key is None:
            raise StorageError("No datasets partition found; run the 'datasets' extractor first.")

        targets: set[SnapshotTarget] = set()
        for record in iter_jsonl_gz(self.ctx.store, key):
            dataset = record.get("dataset") or {}
            snapshot = dataset.get("latestSnapshot") or {}
            # Datasets that were never published have no snapshot to fetch.
            if dataset.get("id") and snapshot.get("tag"):
                targets.add(SnapshotTarget(dataset["id"], snapshot["tag"]))
        return sorted(targets)

    @staticmethod
    def _already_fetched(target: SnapshotTarget, watermark: WatermarkStore) -> bool:
        entry = watermark.get(target.dataset_id)
        return entry is not None and entry.get("snapshot_tag") == target.snapshot_tag

    # ---------------------------------------------------------------- fetching

    def _fetch_all(
        self, targets: list[SnapshotTarget], watermark: WatermarkStore
    ) -> tuple[int, int, list[str]]:
        """Fetch targets in parallel. Returns (fetched, missing, errors)."""
        fetched = missing = 0
        errors: list[str] = []

        with ThreadPoolExecutor(max_workers=self.ctx.settings.max_workers) as pool:
            futures: dict[Future[bool], SnapshotTarget] = {
                pool.submit(self._fetch_one, target): target for target in targets
            }
            try:
                for completed, future in enumerate(as_completed(futures), start=1):
                    target = futures[future]
                    try:
                        has_table = future.result()
                    except EXPECTED_ERRORS as exc:
                        # Watermark is left untouched, so this target is retried next run.
                        log.warning("participants: %s failed: %s", target, exc)
                        errors.append(f"{target.dataset_id}@{target.snapshot_tag}: {exc}")
                        continue

                    if has_table:
                        fetched += 1
                    else:
                        missing += 1
                    watermark.set(
                        target.dataset_id,
                        {
                            "snapshot_tag": target.snapshot_tag,
                            "has_participants_tsv": has_table,
                            "fetched_at": datetime.now(UTC).isoformat(),
                        },
                    )
                    if completed % CHECKPOINT_EVERY == 0:
                        watermark.flush()
            finally:
                # Save progress even if the run is interrupted part-way.
                watermark.flush()

        return fetched, missing, errors

    def _fetch_one(self, target: SnapshotTarget) -> bool:
        """Download one dataset's participant files. Runs on a worker thread.

        Returns False when the dataset has no participants.tsv (BIDS makes it
        optional). The `_meta.json` marker is written last, so the loader only
        ever sees complete folders.
        """
        client, store = self.ctx.client, self.ctx.store
        tsv = client.fetch_dataset_file(target.dataset_id, TSV_FILE)
        if tsv is None:
            return False
        sidecar = client.fetch_dataset_file(target.dataset_id, SIDECAR_FILE)

        def key(filename: str) -> str:
            return layout.participants_file_key(target.dataset_id, target.snapshot_tag, filename)

        store.write_bytes(key(TSV_FILE), tsv)
        if sidecar is not None:
            store.write_bytes(key(SIDECAR_FILE), sidecar)
        write_json(
            store,
            key(layout.PARTICIPANTS_META_FILE),
            {
                "dataset_id": target.dataset_id,
                "snapshot_tag": target.snapshot_tag,
                "run_id": self.ctx.run_id,
                "ingested_at": datetime.now(UTC).isoformat(),
                "files": [TSV_FILE] + ([SIDECAR_FILE] if sidecar is not None else []),
            },
        )
        return True

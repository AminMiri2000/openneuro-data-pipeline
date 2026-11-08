"""Building blocks shared by every extractor.

An extractor moves one entity from a source into the raw layer. New entities
(e.g. per-dataset file listings, events.tsv files) are added by subclassing
`Extractor` and registering it; the pipeline, CLI and future orchestrator pick
it up without further changes.
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from typing import Any, ClassVar

from ..config import Settings
from ..sources.openneuro import OpenNeuroClient
from ..storage import ObjectStore, layout
from ..storage.formats import write_json

log = logging.getLogger(__name__)


def new_run_id(now: datetime) -> str:
    """Sortable, unique run id, e.g. '20261107T203700Z-1a2b3c4d'."""
    return f"{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


@dataclass(frozen=True)
class RunContext:
    """Everything an extractor needs, injected rather than created inside it.

    Injecting the client and store keeps extractors free of set-up code and
    lets tests pass in fakes or a temporary directory.
    """

    settings: Settings
    client: OpenNeuroClient
    store: ObjectStore
    run_id: str
    started_at: datetime

    @classmethod
    def create(cls, settings: Settings, client: OpenNeuroClient, store: ObjectStore) -> RunContext:
        now = datetime.now(UTC)
        return cls(settings, client, store, run_id=new_run_id(now), started_at=now)


@dataclass(frozen=True)
class ExtractParams:
    """User-facing knobs for a run.

    Attributes:
        modality: OpenNeuro modality filter; None means every dataset.
        limit: Cap on items processed, for quick trial runs.
        ingest_date: Partition date to write; defaults to the run's UTC date.
            Setting it explicitly is what makes backfills possible.
    """

    modality: str | None = "EEG"
    limit: int | None = None
    ingest_date: date | None = None


@dataclass
class ExtractResult:
    """Outcome of one extractor run; serialised as the run manifest.

    `errors` are items that failed and will be retried; they make the run
    "not ok". `warnings` are upstream quirks worth recording that need no
    action, so they never fail a scheduled run (avoiding alert fatigue).
    """

    step: str
    run_id: str
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """False when any item failed. Items that succeeded are still kept."""
        return not self.errors


class Extractor(ABC):
    """Template for an extractor.

    Subclasses set `name` (and `depends_on` when they read another extractor's
    output) and implement `extract`. `run` wraps it with logging and the run
    manifest, so every extractor reports itself the same way.
    """

    name: ClassVar[str]
    depends_on: ClassVar[tuple[str, ...]] = ()

    def __init__(self, context: RunContext) -> None:
        self.ctx = context

    @abstractmethod
    def extract(self, params: ExtractParams) -> ExtractResult:
        """Pull data from the source into the raw layer."""

    def run(self, params: ExtractParams) -> ExtractResult:
        log.info("%s: starting run %s", self.name, self.ctx.run_id)
        result = self.extract(params)
        self._write_manifest(params, result)
        level = logging.INFO if result.ok else logging.WARNING
        log.log(
            level,
            "%s: done %s (errors: %d, warnings: %d)",
            self.name,
            result.metrics,
            len(result.errors),
            len(result.warnings),
        )
        return result

    def _write_manifest(self, params: ExtractParams, result: ExtractResult) -> None:
        """Record what the run did, for auditing and debugging."""
        manifest = {
            **asdict(result),
            # Cap the lists: a sample is enough to diagnose, and manifests stay small.
            "errors": result.errors[:50],
            "error_count": len(result.errors),
            "warnings": result.warnings[:50],
            "warning_count": len(result.warnings),
            "params": asdict(params),
            "started_at": self.ctx.started_at.isoformat(),
            "finished_at": datetime.now(UTC).isoformat(),
        }
        write_json(self.ctx.store, layout.manifest_key(self.name, self.ctx.run_id), manifest)

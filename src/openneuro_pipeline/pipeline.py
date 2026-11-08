"""Pipeline facade: the single entry point for extract, load and transform.

The CLI uses it today and the Dagster orchestrator will use it in phase 3, so
neither has to know how clients, storage and extractors are wired together.
Swapping an implementation (e.g. an S3 object store) happens here only.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from .config import Settings
from .extract import ExtractParams, ExtractResult, RunContext, execution_order, get_extractor
from .load import LoadResult, Warehouse
from .sources.openneuro import OpenNeuroClient
from .storage import LocalObjectStore, ObjectStore
from .transform import DbtProject, TransformResult


@dataclass
class PipelineResult:
    """Outcome of a full run: every extractor's result, the load and the transform."""

    extracts: list[ExtractResult] = field(default_factory=list)
    load: LoadResult | None = None
    transform: TransformResult | None = None

    @property
    def ok(self) -> bool:
        return all(result.ok for result in self.extracts)


class Pipeline:
    """Wires settings, source client, raw store and warehouse together."""

    def __init__(
        self,
        settings: Settings,
        client: OpenNeuroClient | None = None,
        store: ObjectStore | None = None,
    ) -> None:
        self.settings = settings
        # Defaults are free and local; callers (and tests) may inject others.
        self.client = client or OpenNeuroClient(settings)
        self.store = store or LocalObjectStore(settings.raw_dir)
        self.warehouse = Warehouse(settings, self.store)
        self.dbt = DbtProject(settings)

    def extract(
        self, names: Iterable[str] | None = None, params: ExtractParams | None = None
    ) -> list[ExtractResult]:
        """Run the named extractors (plus their dependencies) in order.

        All extractors in one call share a run id, which ties their manifests
        together. Each extractor still runs even if an earlier one reported
        item-level errors, because partial data is better than none; a source
        outage raises and stops the run.
        """
        params = params or ExtractParams()
        context = RunContext.create(self.settings, self.client, self.store)
        return [get_extractor(name)(context).run(params) for name in execution_order(names)]

    def load(self, full_refresh: bool = False) -> LoadResult:
        return self.warehouse.load(full_refresh=full_refresh)

    def transform(self, select: str | None = None, full_refresh: bool = False) -> TransformResult:
        """Build the dbt models and run their tests."""
        return self.dbt.build(select=select, full_refresh=full_refresh)

    def run(
        self,
        params: ExtractParams | None = None,
        full_refresh: bool = False,
        transform: bool = True,
    ) -> PipelineResult:
        """Full batch: every extractor, then the warehouse load, then dbt.

        `full_refresh` rebuilds both the raw tables and the incremental dbt
        models, which is how a backfill older than the incremental look-back
        window is applied.
        """
        result = PipelineResult(extracts=self.extract(params=params))
        result.load = self.load(full_refresh=full_refresh)
        if transform:
            result.transform = self.transform(full_refresh=full_refresh)
        return result

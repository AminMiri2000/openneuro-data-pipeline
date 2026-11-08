"""Extract the OpenNeuro dataset catalogue.

Strategy: daily full snapshot. The catalogue is small (thousands of rows), so
pulling it whole is cheaper and simpler than tracking changes, and one
partition per day gives the warehouse a free history of every dataset.

Idempotency: the output key depends only on (ingest_date, modality), so a
re-run overwrites the same partition; a failed run writes nothing.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

from ..sources.openneuro import PageStats
from ..storage import layout
from ..storage.formats import JSON, write_jsonl_gz
from .base import Extractor, ExtractParams, ExtractResult
from .registry import register

SOURCE_NAME = "openneuro_graphql.datasets"


@register
class DatasetsExtractor(Extractor):
    """Writes the full catalogue for one modality as one daily partition."""

    name = "datasets"

    def extract(self, params: ExtractParams) -> ExtractResult:
        ingest_date = params.ingest_date or self.ctx.started_at.date()
        key = layout.datasets_key(ingest_date, params.modality)
        stats = PageStats()

        records = write_jsonl_gz(self.ctx.store, key, self._records(params, stats))

        return ExtractResult(
            step=self.name,
            run_id=self.ctx.run_id,
            metrics={
                "key": key,
                "ingest_date": ingest_date.isoformat(),
                "modality": layout.modality_key(params.modality),
                "records": records,
                "pages": stats.pages,
                "null_nodes": stats.null_nodes,
            },
            # Partial GraphQL errors are recorded but do not fail the run: the
            # nodes OpenNeuro could resolve are still worth keeping.
            warnings=[f"graphql: {message}" for message in stats.graphql_errors],
        )

    def _records(self, params: ExtractParams, stats: PageStats) -> Iterator[JSON]:
        """Wrap each API node in an envelope of lineage fields (prefixed '_')."""
        for node in self.ctx.client.iter_datasets(params.modality, params.limit, stats):
            yield {
                "_run_id": self.ctx.run_id,
                "_ingested_at": self.ctx.started_at.isoformat(),
                "_source": SOURCE_NAME,
                "_modality_filter": params.modality,
                "dataset": node,  # kept exactly as the API returned it
            }


def latest_partition_key(keys: list[str], modality: str | None) -> str | None:
    """Pick the newest datasets partition for a modality from a list of keys."""
    wanted = layout.modality_key(modality)
    candidates = [
        key
        for key in keys
        if key.endswith(layout.DATASETS_FILE)
        and layout.parse_partitions(key).get("modality") == wanted
    ]
    return max(candidates, key=_ingest_date, default=None)


def _ingest_date(key: str) -> date:
    return date.fromisoformat(layout.parse_partitions(key)["ingest_date"])

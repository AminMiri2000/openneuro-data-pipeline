"""Extract and load assets.

No `from __future__ import annotations` here: Dagster reads the type hints
at runtime and needs real classes, not strings.

Asset graph (dbt models continue it, see dbt_assets.py):

    raw_files/openneuro_catalogue ──> raw_files/openneuro_participant_files
             │                                   │
             └──────> openneuro/openneuro_datasets <── load ──┐
                      openneuro/openneuro_participants  <─────┤
                      openneuro/openneuro_participant_columns ┘

The load step's asset keys equal the dbt source keys (`source_name/table`),
so Dagster joins this graph to the dbt models automatically.
"""

from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Backoff,
    MaterializeResult,
    RetryPolicy,
    asset,
    multi_asset,
)

from ..extract import ExtractParams, ExtractResult, RunContext, get_extractor
from ..load.tables import TABLE_LOADERS
from .resources import PipelineResource

# Default modality for scheduled runs; matches the CLI default.
MODALITY = "EEG"

CATALOGUE_KEY = AssetKey(["raw_files", "openneuro_catalogue"])
PARTICIPANT_FILES_KEY = AssetKey(["raw_files", "openneuro_participant_files"])

# A public API that is briefly down should not fail the daily run: retry with
# growing pauses (1, 2, then 4 minutes) before giving up.
SOURCE_RETRY = RetryPolicy(max_retries=3, delay=60, backoff=Backoff.EXPONENTIAL)

# Which raw files each raw table is loaded from (lineage in the UI).
_RAW_TABLE_UPSTREAM = {
    "raw.openneuro_datasets": CATALOGUE_KEY,
    "raw.openneuro_participants": PARTICIPANT_FILES_KEY,
    "raw.openneuro_participant_columns": PARTICIPANT_FILES_KEY,
}


def raw_table_key(table: str) -> AssetKey:
    """Asset key of a raw table, identical to its dbt source key.

    "raw.openneuro_datasets" -> openneuro/openneuro_datasets
    """
    return AssetKey(["openneuro", table.split(".", 1)[1]])


def _extract_metadata(result: ExtractResult) -> dict[str, Any]:
    """Run metrics shown on the asset's page, plus error and warning counts."""
    return {
        **{name: value for name, value in result.metrics.items() if name != "key"},
        "run_id": result.run_id,
        "error_count": len(result.errors),
        "warning_count": len(result.warnings),
    }


@asset(
    key=CATALOGUE_KEY,
    group_name="extract",
    retry_policy=SOURCE_RETRY,
    kinds={"python", "duckdb"},
    description=(
        "Today's OpenNeuro dataset catalogue (one modality), saved as a gzipped "
        "JSON Lines partition in the raw layer."
    ),
)
def openneuro_catalogue(
    context: AssetExecutionContext, pipeline: PipelineResource
) -> MaterializeResult:
    [result] = pipeline.get_pipeline().extract(["datasets"], ExtractParams(modality=MODALITY))
    for warning in result.warnings:
        context.log.warning(warning)
    return MaterializeResult(metadata=_extract_metadata(result))


@asset(
    key=PARTICIPANT_FILES_KEY,
    deps=[CATALOGUE_KEY],
    group_name="extract",
    retry_policy=SOURCE_RETRY,
    kinds={"python"},
    check_specs=[
        AssetCheckSpec(
            name="no_failed_downloads",
            asset=PARTICIPANT_FILES_KEY,
            description="Every changed dataset's participant files were downloaded.",
        )
    ],
    description=(
        "participants.tsv and participants.json for each dataset whose snapshot "
        "changed since the last successful download."
    ),
)
def openneuro_participant_files(
    context: AssetExecutionContext, pipeline: PipelineResource
) -> MaterializeResult:
    # Run only this extractor: the catalogue is its own upstream asset, so
    # Pipeline.extract (which adds dependencies) would fetch it twice.
    runner = pipeline.get_pipeline()
    run_context = RunContext.create(runner.settings, runner.client, runner.store)
    result = get_extractor("participants")(run_context).run(ExtractParams(modality=MODALITY))

    for error in result.errors[:20]:
        context.log.warning(error)
    # Failed datasets are retried automatically next run, so they warn
    # rather than fail the run and block the load of everything that worked.
    check = AssetCheckResult(
        check_name="no_failed_downloads",
        passed=result.ok,
        severity=AssetCheckSeverity.WARN,
        metadata={"failed_datasets": len(result.errors), "sample": result.errors[:5]},
    )
    return MaterializeResult(metadata=_extract_metadata(result), check_results=[check])


@multi_asset(
    specs=[
        AssetSpec(
            raw_table_key(loader.table),
            deps=[_RAW_TABLE_UPSTREAM[loader.table]],
            group_name="load",
            kinds={"duckdb"},
            description=f"`{loader.table}`, loaded incrementally from the raw layer.",
        )
        for loader in TABLE_LOADERS
    ],
    can_subset=False,  # one transactional load per run keeps the load log consistent
)
# No return annotation: Dagster only accepts a tuple annotation (or none) on a
# multi-output asset, and this one is a generator.
def raw_tables(context: AssetExecutionContext, pipeline: PipelineResource):  # type: ignore[no-untyped-def]
    """Load new or changed raw partitions into DuckDB's `raw` schema."""
    result = pipeline.get_pipeline().load()
    for table, stats in result.tables.items():
        yield MaterializeResult(
            asset_key=raw_table_key(table),
            metadata={
                "loaded_partitions": stats.loaded_partitions,
                "skipped_partitions": stats.skipped_partitions,
                "rows_inserted": stats.rows_inserted,
            },
        )

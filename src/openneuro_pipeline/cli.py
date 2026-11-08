"""Command-line interface: `openneuro-pipeline <command>`.

Deliberately thin: it parses options, calls `Pipeline`, prints a JSON summary
and sets the exit code. No business logic lives here.

Exit codes: 0 success, 1 some items failed (the rest were kept),
2 the run could not complete (configuration, source or storage error).
"""

from __future__ import annotations

import json
import sys
from dataclasses import asdict
from datetime import date
from typing import Any

import click
import duckdb

from .config import Settings
from .errors import PipelineError
from .extract import ExtractParams, ExtractResult, available
from .logging_utils import configure_logging
from .pipeline import Pipeline
from .sources.openneuro.queries import DATASETS_QUERY

EXIT_PARTIAL_FAILURE = 1
EXIT_ERROR = 2


def _echo_json(payload: Any) -> None:
    click.echo(json.dumps(payload, indent=2, default=str))


def _build_params(modality: str, limit: int | None, ingest_date: date | None) -> ExtractParams:
    return ExtractParams(
        modality=None if modality.lower() == "all" else modality,
        limit=limit,
        ingest_date=ingest_date,
    )


def _exit_for(results: list[ExtractResult]) -> None:
    if not all(r.ok for r in results):
        sys.exit(EXIT_PARTIAL_FAILURE)


# Options shared by the extract-style commands.
_modality = click.option(
    "--modality",
    default="EEG",
    show_default=True,
    help="OpenNeuro modality filter (EEG, MEG, MRI, iEEG, ...) or 'all'.",
)
_limit = click.option("--limit", type=int, help="Cap items processed, for quick trials.")
_ingest_date = click.option(
    "--ingest-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Partition date to write (default: today, UTC). Used for backfills.",
)
_full_refresh = click.option(
    "--full-refresh", is_flag=True, help="Drop raw tables and rebuild from raw files."
)


class _Group(click.Group):
    """Turns expected pipeline errors into a clean message and exit code 2."""

    def invoke(self, ctx: click.Context) -> Any:
        try:
            return super().invoke(ctx)
        except PipelineError as exc:
            click.echo(f"error: {exc}", err=True)
            sys.exit(EXIT_ERROR)


@click.group(cls=_Group)
@click.option("-v", "--verbose", is_flag=True, help="Debug logging.")
@click.pass_context
def main(ctx: click.Context, verbose: bool) -> None:
    """OpenNeuro metadata pipeline."""
    configure_logging(verbose)
    ctx.obj = Pipeline(Settings.from_env())


@main.command("check-api")
@click.pass_obj
def check_api(pipeline: Pipeline) -> None:
    """Fetch one small page to confirm the API and query still work."""
    body = pipeline.client.graphql(DATASETS_QUERY, {"first": 2, "after": None, "modality": "EEG"})
    click.echo(json.dumps(body, indent=2)[:3000])


@main.command("extract")
@click.argument("names", nargs=-1, type=click.Choice(available()))
@_modality
@_limit
@_ingest_date
@click.pass_obj
def extract_cmd(
    pipeline: Pipeline, names: tuple[str, ...], modality: str, limit: int | None, ingest_date: Any
) -> None:
    """Run extractors (default: all). Dependencies are added automatically."""
    params = _build_params(modality, limit, ingest_date.date() if ingest_date else None)
    results = pipeline.extract(names or None, params)
    _echo_json([asdict(r) for r in results])
    _exit_for(results)


@main.command("load")
@_full_refresh
@click.pass_obj
def load_cmd(pipeline: Pipeline, full_refresh: bool) -> None:
    """Load new or changed raw partitions into DuckDB."""
    _echo_json(asdict(pipeline.load(full_refresh=full_refresh)))


@main.command("query")
@click.argument("sql")
@click.pass_obj
def query_cmd(pipeline: Pipeline, sql: str) -> None:
    """Run a read-only SQL query against the warehouse and print the result.

    Opened read-only, so a query can never modify loaded data.
    """
    if not pipeline.settings.warehouse_path.exists():
        click.echo("error: no warehouse yet; run `make run` first", err=True)
        sys.exit(EXIT_ERROR)
    try:
        with pipeline.warehouse.connect(read_only=True) as con:
            click.echo(str(con.sql(sql)))
    except duckdb.Error as exc:  # bad SQL or an attempted write: a message, not a traceback
        click.echo(f"query error: {exc}", err=True)
        sys.exit(EXIT_ERROR)


@main.command("run")
@_modality
@_limit
@_ingest_date
@_full_refresh
@click.pass_obj
def run_cmd(
    pipeline: Pipeline, modality: str, limit: int | None, ingest_date: Any, full_refresh: bool
) -> None:
    """Full batch: all extractors, then load."""
    params = _build_params(modality, limit, ingest_date.date() if ingest_date else None)
    result = pipeline.run(params, full_refresh=full_refresh)
    _echo_json(
        {
            "extracts": [asdict(r) for r in result.extracts],
            "load": asdict(result.load) if result.load else None,
        }
    )
    _exit_for(result.extracts)


if __name__ == "__main__":
    main()

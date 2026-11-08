"""Command-line entry point: `openneuro-pipeline <command>`."""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict

import click

from .client import DATASETS_QUERY, OpenNeuroClient
from .config import get_settings
from .ingest.datasets import ingest_datasets
from .ingest.participants import ingest_participants
from .load import load_raw


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="Debug logging.")
def main(verbose: bool) -> None:
    """OpenNeuro metadata pipeline."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


modality_option = click.option(
    "--modality",
    default="EEG",
    show_default=True,
    help="OpenNeuro modality filter (EEG, MEG, MRI, iEEG, ...). Use 'all' for no filter.",
)
limit_option = click.option("--limit", type=int, default=None, help="Cap rows, for quick tests.")


def _modality(value: str) -> str | None:
    return None if value.lower() == "all" else value


@main.command("check-api")
def check_api() -> None:
    """Fetch one page of datasets to confirm the API and query still work."""
    client = OpenNeuroClient(get_settings())
    body = client.graphql(DATASETS_QUERY, {"first": 2, "after": None, "modality": "EEG"})
    click.echo(json.dumps(body, indent=2)[:3000])


@main.group()
def ingest() -> None:
    """Extract from OpenNeuro into the raw layer."""


@ingest.command("datasets")
@modality_option
@limit_option
def ingest_datasets_cmd(modality: str, limit: int | None) -> None:
    settings = get_settings()
    result = ingest_datasets(OpenNeuroClient(settings), settings, _modality(modality), limit)
    click.echo(json.dumps(asdict(result), indent=2))


@ingest.command("participants")
@modality_option
@limit_option
def ingest_participants_cmd(modality: str, limit: int | None) -> None:
    settings = get_settings()
    result = ingest_participants(OpenNeuroClient(settings), settings, _modality(modality), limit)
    click.echo(json.dumps(asdict(result), indent=2))
    if result.failed:
        sys.exit(1)  # surface partial failure to the scheduler; succeeded datasets are kept


@main.command("load")
def load_cmd() -> None:
    """Rebuild the DuckDB raw tables from the raw files."""
    click.echo(json.dumps(asdict(load_raw(get_settings())), indent=2))


@main.command("run")
@modality_option
@limit_option
def run_all(modality: str, limit: int | None) -> None:
    """Full batch: datasets -> participants -> load."""
    settings = get_settings()
    client = OpenNeuroClient(settings)
    mod = _modality(modality)
    ds = ingest_datasets(client, settings, mod, limit)
    parts = ingest_participants(client, settings, mod)
    loaded = load_raw(settings)
    click.echo(
        json.dumps(
            {"datasets": asdict(ds), "participants": asdict(parts), "load": asdict(loaded)},
            indent=2,
        )
    )
    if parts.failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

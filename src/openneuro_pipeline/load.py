"""Load the raw layer into DuckDB's `raw` schema.

The raw tables are rebuilt from the raw files on every run, inside one
transaction. That makes loading idempotent and lets you replay history: delete
the warehouse file, run `load`, and you get the same tables back.

Raw tables keep the source payload untouched (as JSON / strings). Typing,
renaming and cleaning happen later in dbt (staging layer), so a change in the
API's shape never breaks loading, only a model you can see and test.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb

from .config import Settings

log = logging.getLogger(__name__)

BIDS_NULLS = {"n/a", "N/A", ""}


@dataclass
class LoadResult:
    datasets_rows: int
    participants_rows: int
    participant_columns_rows: int


def connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(settings.warehouse_path))


def load_raw(settings: Settings) -> LoadResult:
    con = connect(settings)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        datasets_rows = _load_datasets(con, settings)
        participants_rows, columns_rows = _load_participants(con, settings)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()

    result = LoadResult(datasets_rows, participants_rows, columns_rows)
    log.info("load: %s", result)
    return result


def _load_datasets(con: duckdb.DuckDBPyConnection, settings: Settings) -> int:
    pattern = settings.raw_dir / "datasets" / "*" / "*" / "datasets.jsonl.gz"
    con.execute(
        """
        CREATE OR REPLACE TABLE raw.openneuro_datasets (
            ingest_date      DATE,
            modality_filter  VARCHAR,
            run_id           VARCHAR,
            ingested_at      TIMESTAMPTZ,
            dataset_id       VARCHAR,
            payload          JSON,
            source_file      VARCHAR
        )
        """
    )
    if not list(settings.raw_dir.glob("datasets/*/*/datasets.jsonl.gz")):
        return 0
    con.execute(
        """
        INSERT INTO raw.openneuro_datasets
        SELECT
            CAST(ingest_date AS DATE),
            modality,
            json ->> '$._run_id',
            CAST(json ->> '$._ingested_at' AS TIMESTAMPTZ),
            json -> '$.dataset' ->> '$.id',
            json -> '$.dataset',
            filename
        FROM read_ndjson_objects(?, hive_partitioning = true, filename = true)
        """,
        [str(pattern)],
    )
    return con.execute("SELECT count(*) FROM raw.openneuro_datasets").fetchone()[0]


def _load_participants(con: duckdb.DuckDBPyConnection, settings: Settings) -> tuple[int, int]:
    con.execute(
        """
        CREATE OR REPLACE TABLE raw.openneuro_participants (
            dataset_id      VARCHAR,
            snapshot_tag    VARCHAR,
            row_number      INTEGER,
            participant_id  VARCHAR,
            attributes      JSON,
            ingested_at     TIMESTAMPTZ,
            source_file     VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE raw.openneuro_participant_columns (
            dataset_id      VARCHAR,
            snapshot_tag    VARCHAR,
            column_name     VARCHAR,
            definition      JSON,
            source_file     VARCHAR
        )
        """
    )

    participant_rows: list[tuple] = []
    column_rows: list[tuple] = []
    root = settings.raw_dir / "participants"
    for meta_path in sorted(root.glob("dataset_id=*/snapshot_tag=*/_meta.json")):
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        folder = meta_path.parent
        tsv = folder / "participants.tsv"
        if tsv.exists():
            participant_rows.extend(_parse_participants_tsv(tsv, meta))
        sidecar = folder / "participants.json"
        if sidecar.exists():
            column_rows.extend(_parse_sidecar(sidecar, meta))

    if participant_rows:
        con.executemany(
            "INSERT INTO raw.openneuro_participants VALUES (?, ?, ?, ?, ?, ?, ?)",
            participant_rows,
        )
    if column_rows:
        con.executemany(
            "INSERT INTO raw.openneuro_participant_columns VALUES (?, ?, ?, ?, ?)",
            column_rows,
        )
    return len(participant_rows), len(column_rows)


def _parse_participants_tsv(path: Path, meta: dict) -> list[tuple]:
    """BIDS participants.tsv -> one row per participant, all other columns kept as JSON."""
    text = path.read_bytes().decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    rows = []
    for i, record in enumerate(reader, start=1):
        clean = {
            (k or "").strip(): (None if v is None or v.strip() in BIDS_NULLS else v.strip())
            for k, v in record.items()
            if k is not None
        }
        participant_id = clean.pop("participant_id", None)
        rows.append(
            (
                meta["dataset_id"],
                meta["snapshot_tag"],
                i,
                participant_id,
                json.dumps(clean, ensure_ascii=False),
                meta["ingested_at"],
                str(path),
            )
        )
    return rows


def _parse_sidecar(path: Path, meta: dict) -> list[tuple]:
    try:
        doc = json.loads(path.read_bytes().decode("utf-8-sig", errors="replace"))
    except json.JSONDecodeError:
        log.warning("load: unreadable sidecar %s, skipped", path)
        return []
    if not isinstance(doc, dict):
        return []
    return [
        (
            meta["dataset_id"],
            meta["snapshot_tag"],
            col,
            json.dumps(defn, ensure_ascii=False),
            str(path),
        )
        for col, defn in doc.items()
    ]

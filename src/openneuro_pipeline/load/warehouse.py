"""Load the raw layer into the DuckDB warehouse, incrementally.

How it works
------------
Every raw table is split into partitions (a day of catalogue data, one
dataset's participant folder, ...). A load log records the fingerprint of each
partition last loaded. On each run:

* new partition       -> inserted
* changed fingerprint -> its rows are deleted and re-inserted (replace in place)
* unchanged           -> skipped

Each partition is loaded in its own transaction together with its log entry,
so a crash never leaves a partition half-loaded, and a re-run continues from
the first partition that was not committed. Load time therefore grows with
the amount of *new* data, not with the size of the history.

`full_refresh=True` drops the raw tables and the log and rebuilds from the raw
files, which is how you recover from a schema change.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import duckdb

from ..config import Settings
from ..storage import ObjectStore
from .tables import TABLE_LOADERS, RawTableLoader, SourcePartition

log = logging.getLogger(__name__)

RAW_SCHEMA = "raw"
LOAD_LOG_TABLE = f"{RAW_SCHEMA}._load_log"


@dataclass
class TableLoadStats:
    """Per-table counters for one load run."""

    loaded_partitions: int = 0
    skipped_partitions: int = 0
    rows_inserted: int = 0


@dataclass
class LoadResult:
    """Outcome of a load run, keyed by fully qualified table name."""

    tables: dict[str, TableLoadStats] = field(default_factory=dict)


class Warehouse:
    """DuckDB warehouse holding the `raw` schema.

    DuckDB allows a single writer at a time, which suits a batch pipeline. A
    multi-writer setup would swap this class for one targeting a server
    warehouse (Postgres, BigQuery, ...); extractors are unaffected.
    """

    def __init__(self, settings: Settings, store: ObjectStore) -> None:
        self._settings = settings
        self._store = store
        self._loaders: list[RawTableLoader] = [
            loader_cls(store, settings.insert_batch_size) for loader_cls in TABLE_LOADERS
        ]

    def connect(self, read_only: bool = False) -> duckdb.DuckDBPyConnection:
        path = self._settings.warehouse_path
        path.parent.mkdir(parents=True, exist_ok=True)
        return duckdb.connect(str(path), read_only=read_only)

    def load(self, full_refresh: bool = False) -> LoadResult:
        result = LoadResult()
        with self.connect() as con:
            if full_refresh:
                self._drop_raw_tables(con)
            self._create_schema(con)
            loaded = self._loaded_fingerprints(con)
            for loader in self._loaders:
                result.tables[loader.table] = self._load_table(con, loader, loaded)
        log.info("load: %s", result)
        return result

    # ------------------------------------------------------------------ steps

    def _create_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}")
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {LOAD_LOG_TABLE} (
                table_name    VARCHAR NOT NULL,
                partition_key VARCHAR NOT NULL,
                fingerprint   VARCHAR NOT NULL,
                row_count     BIGINT  NOT NULL,
                loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (table_name, partition_key)
            )
            """
        )
        for loader in self._loaders:
            loader.create_table(con)

    def _drop_raw_tables(self, con: duckdb.DuckDBPyConnection) -> None:
        log.info("load: full refresh, dropping raw tables")
        con.execute(f"DROP TABLE IF EXISTS {LOAD_LOG_TABLE}")
        for loader in self._loaders:
            con.execute(f"DROP TABLE IF EXISTS {loader.table}")

    @staticmethod
    def _loaded_fingerprints(con: duckdb.DuckDBPyConnection) -> dict[tuple[str, str], str]:
        rows = con.execute(
            f"SELECT table_name, partition_key, fingerprint FROM {LOAD_LOG_TABLE}"
        ).fetchall()
        return {(table, key): fingerprint for table, key, fingerprint in rows}

    def _load_table(
        self,
        con: duckdb.DuckDBPyConnection,
        loader: RawTableLoader,
        loaded: dict[tuple[str, str], str],
    ) -> TableLoadStats:
        stats = TableLoadStats()
        for partition in loader.partitions():
            if loaded.get((loader.table, partition.key)) == partition.fingerprint:
                stats.skipped_partitions += 1
                continue
            stats.rows_inserted += self._replace_partition(con, loader, partition)
            stats.loaded_partitions += 1
        return stats

    @staticmethod
    def _replace_partition(
        con: duckdb.DuckDBPyConnection, loader: RawTableLoader, partition: SourcePartition
    ) -> int:
        """Swap one partition's rows and log entry atomically."""
        con.execute("BEGIN TRANSACTION")
        try:
            loader.delete_partition(con, partition.key)
            rows = loader.insert_partition(con, partition)
            con.execute(
                f"""
                INSERT OR REPLACE INTO {LOAD_LOG_TABLE}
                    (table_name, partition_key, fingerprint, row_count, loaded_at)
                VALUES (?, ?, ?, ?, now())
                """,
                [loader.table, partition.key, partition.fingerprint, rows],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return rows

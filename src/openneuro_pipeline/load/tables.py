"""Raw-table loaders: one class per DuckDB table in the `raw` schema.

Each loader knows three things: its table definition, how to discover source
partitions in the raw layer, and how to insert one partition. The shared base
class handles everything else (change detection, replace-in-place, batching).

Adding a table means adding a subclass and listing it in `TABLE_LOADERS`.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import Any, ClassVar

import duckdb

from ..storage import ObjectStore, layout
from ..storage.formats import read_json
from .bids import parse_participants_tsv, parse_sidecar

PARTITION_COLUMN = "_partition_key"


@dataclass(frozen=True)
class SourcePartition:
    """A unit of loading: replaced as a whole when its source changes.

    Attributes:
        key: Stable identity of the partition (a raw-layer folder or file key).
        fingerprint: Changes when the source content changes.
    """

    key: str
    fingerprint: str


def batched(rows: Iterable[Sequence[Any]], size: int) -> Iterator[list[Sequence[Any]]]:
    """Split an iterable into lists of at most `size` items (bounded memory)."""
    iterator = iter(rows)
    while batch := list(islice(iterator, size)):
        yield batch


class RawTableLoader(ABC):
    """Base class for loading one raw table partition by partition."""

    table: ClassVar[str]  # fully qualified, e.g. "raw.openneuro_datasets"
    columns_ddl: ClassVar[str]  # column list, without the partition column

    def __init__(self, store: ObjectStore, batch_size: int) -> None:
        self.store = store
        self.batch_size = batch_size

    def create_table(self, con: duckdb.DuckDBPyConnection) -> None:
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table} "
            f"({self.columns_ddl}, {PARTITION_COLUMN} VARCHAR NOT NULL)"
        )

    def delete_partition(self, con: duckdb.DuckDBPyConnection, partition_key: str) -> None:
        con.execute(f"DELETE FROM {self.table} WHERE {PARTITION_COLUMN} = ?", [partition_key])

    @abstractmethod
    def partitions(self) -> Iterator[SourcePartition]:
        """Discover the partitions currently present in the raw layer."""

    @abstractmethod
    def insert_partition(self, con: duckdb.DuckDBPyConnection, partition: SourcePartition) -> int:
        """Insert all rows of one partition and return how many were inserted."""

    def _insert_rows(
        self, con: duckdb.DuckDBPyConnection, rows: Iterable[Sequence[Any]], width: int
    ) -> int:
        """Insert rows in batches; `width` excludes the partition column."""
        placeholders = ", ".join(["?"] * (width + 1))
        count = 0
        for batch in batched(rows, self.batch_size):
            con.executemany(f"INSERT INTO {self.table} VALUES ({placeholders})", batch)
            count += len(batch)
        return count


# --------------------------------------------------------------------- datasets


class DatasetsTableLoader(RawTableLoader):
    """Catalogue snapshots: one row per dataset per ingest day."""

    table = "raw.openneuro_datasets"
    columns_ddl = """
        ingest_date     DATE,
        modality_filter VARCHAR,
        run_id          VARCHAR,
        ingested_at     TIMESTAMPTZ,
        dataset_id      VARCHAR,
        payload         JSON
    """

    def partitions(self) -> Iterator[SourcePartition]:
        for key in self.store.list_keys(layout.datasets_prefix()):
            if key.endswith(layout.DATASETS_FILE):
                yield SourcePartition(key, self.store.stat(key).fingerprint)

    def insert_partition(self, con: duckdb.DuckDBPyConnection, partition: SourcePartition) -> int:
        # DuckDB reads the gzipped JSON Lines natively: no rows pass through
        # Python, so this scales with DuckDB rather than with the interpreter.
        values = layout.parse_partitions(partition.key)
        # DuckDB answers an INSERT with the number of rows it inserted.
        inserted = con.execute(
            f"""
            INSERT INTO {self.table}
            SELECT
                CAST(? AS DATE),
                ?,
                json ->> '$._run_id',
                CAST(json ->> '$._ingested_at' AS TIMESTAMPTZ),
                json -> '$.dataset' ->> '$.id',
                json -> '$.dataset',
                ?
            FROM read_ndjson_objects(?)
            """,
            [
                values["ingest_date"],
                values["modality"],
                partition.key,
                self.store.uri(partition.key),
            ],
        ).fetchone()
        return int(inserted[0]) if inserted else 0


# ----------------------------------------------------------------- participants


class _ParticipantFolderLoader(RawTableLoader, ABC):
    """Shared discovery for tables built from participant folders.

    A folder counts as a partition only once its `_meta.json` marker exists
    (it is written last), and the marker's fingerprint changes whenever the
    folder is re-fetched.
    """

    def partitions(self) -> Iterator[SourcePartition]:
        for key in self.store.list_keys(layout.participants_prefix()):
            if key.endswith(f"/{layout.PARTICIPANTS_META_FILE}"):
                folder = key.rsplit("/", 1)[0]
                yield SourcePartition(folder, self.store.stat(key).fingerprint)

    def _meta(self, partition: SourcePartition) -> dict[str, Any]:
        meta: dict[str, Any] = read_json(
            self.store, f"{partition.key}/{layout.PARTICIPANTS_META_FILE}"
        )
        return meta


class ParticipantsTableLoader(_ParticipantFolderLoader):
    """participants.tsv: one row per participant, extra columns as JSON."""

    table = "raw.openneuro_participants"
    columns_ddl = """
        dataset_id     VARCHAR,
        snapshot_tag   VARCHAR,
        row_number     INTEGER,
        participant_id VARCHAR,
        attributes     JSON,
        ingested_at    TIMESTAMPTZ
    """

    def insert_partition(self, con: duckdb.DuckDBPyConnection, partition: SourcePartition) -> int:
        tsv_key = f"{partition.key}/participants.tsv"
        if not self.store.exists(tsv_key):
            return 0
        meta = self._meta(partition)
        rows = (
            (
                meta["dataset_id"],
                meta["snapshot_tag"],
                row.row_number,
                row.participant_id,
                json.dumps(row.attributes, ensure_ascii=False),
                meta["ingested_at"],
                partition.key,
            )
            for row in parse_participants_tsv(self.store.read_bytes(tsv_key))
        )
        return self._insert_rows(con, rows, width=6)


class ParticipantColumnsTableLoader(_ParticipantFolderLoader):
    """participants.json: one row per documented column (BIDS data dictionary)."""

    table = "raw.openneuro_participant_columns"
    columns_ddl = """
        dataset_id   VARCHAR,
        snapshot_tag VARCHAR,
        column_name  VARCHAR,
        definition   JSON
    """

    def insert_partition(self, con: duckdb.DuckDBPyConnection, partition: SourcePartition) -> int:
        sidecar_key = f"{partition.key}/participants.json"
        if not self.store.exists(sidecar_key):
            return 0
        meta = self._meta(partition)
        rows = (
            (
                meta["dataset_id"],
                meta["snapshot_tag"],
                column,
                json.dumps(definition, ensure_ascii=False),
                partition.key,
            )
            for column, definition in parse_sidecar(self.store.read_bytes(sidecar_key))
        )
        return self._insert_rows(con, rows, width=4)


TABLE_LOADERS: tuple[type[RawTableLoader], ...] = (
    DatasetsTableLoader,
    ParticipantsTableLoader,
    ParticipantColumnsTableLoader,
)

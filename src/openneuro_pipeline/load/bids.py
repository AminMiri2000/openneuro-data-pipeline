"""Parsers for BIDS tabular files (pure functions, no I/O).

Kept free of storage and database code so they can be unit-tested with plain
bytes and reused by any loader.
"""

from __future__ import annotations

import csv
import io
import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

# BIDS writes missing values as 'n/a'; treat empty cells the same way.
BIDS_NULL_VALUES = frozenset({"n/a", "N/A", ""})
PARTICIPANT_ID_COLUMN = "participant_id"


@dataclass(frozen=True)
class ParticipantRow:
    """One participant: the id plus every other column, untyped."""

    row_number: int
    participant_id: str | None
    attributes: dict[str, str | None]


def _decode(data: bytes) -> str:
    # 'utf-8-sig' strips a byte-order mark some editors add; bad bytes are
    # replaced rather than failing the whole file.
    return data.decode("utf-8-sig", errors="replace")


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return None if value in BIDS_NULL_VALUES else value


def parse_participants_tsv(data: bytes) -> Iterator[ParticipantRow]:
    """Parse participants.tsv into rows.

    Column sets differ between datasets, so non-id columns stay as a dict of
    strings; typing them is the warehouse models' job, not the loader's.
    """
    reader = csv.DictReader(io.StringIO(_decode(data)), delimiter="\t")
    for row_number, record in enumerate(reader, start=1):
        attributes = {
            column.strip(): _clean(value)
            for column, value in record.items()
            # DictReader puts surplus cells under a None key; drop them.
            if column is not None and column.strip()
        }
        participant_id = attributes.pop(PARTICIPANT_ID_COLUMN, None)
        yield ParticipantRow(row_number, participant_id, attributes)


def parse_sidecar(data: bytes) -> Iterator[tuple[str, Any]]:
    """Parse participants.json into (column_name, definition) pairs.

    An unreadable sidecar is logged and skipped: it documents the columns but
    the participant data itself is still usable without it.
    """
    try:
        document = json.loads(_decode(data))
    except json.JSONDecodeError as exc:
        log.warning("bids: unreadable participants.json skipped (%s)", exc)
        return
    if isinstance(document, dict):
        yield from document.items()

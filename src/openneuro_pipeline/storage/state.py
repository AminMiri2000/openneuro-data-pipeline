"""Persistent watermarks for incremental extraction."""

from __future__ import annotations

from typing import Any

from . import layout
from .base import ObjectStore
from .formats import read_json, write_json


class WatermarkStore:
    """Remembers, per entity key, the last version that was processed.

    Example: for participants the key is a dataset id and the value records the
    snapshot tag last fetched. Entries are only written after the work for that
    key succeeded, so a failed key is retried on the next run.

    Not thread-safe by design: workers report results to the coordinating
    thread, which is the only writer. That keeps this class simple.
    """

    def __init__(self, store: ObjectStore, name: str) -> None:
        self._store = store
        self._key = layout.state_key(name)
        self._entries: dict[str, dict[str, Any]] = (
            read_json(store, self._key) if store.exists(self._key) else {}
        )

    def get(self, key: str) -> dict[str, Any] | None:
        return self._entries.get(key)

    def set(self, key: str, value: dict[str, Any]) -> None:
        self._entries[key] = value

    def flush(self) -> None:
        """Persist all entries atomically."""
        write_json(self._store, self._key, self._entries)

    def __contains__(self, key: object) -> bool:
        return key in self._entries

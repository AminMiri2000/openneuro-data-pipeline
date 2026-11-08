"""Serialisation helpers for raw-layer file formats."""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable, Iterator
from typing import Any

from .base import ObjectStore

JSON = dict[str, Any]


def write_jsonl_gz(store: ObjectStore, key: str, records: Iterable[JSON]) -> int:
    """Stream records to `key` as gzipped JSON Lines and return the count.

    Records are written one at a time, so memory use does not grow with the
    number of records. `mtime=0` makes the gzip header deterministic: the same
    input always produces byte-identical output.
    """
    count = 0
    with store.open_writer(key) as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
        for record in records:
            line = json.dumps(record, sort_keys=True, ensure_ascii=False)
            gz.write(line.encode("utf-8") + b"\n")
            count += 1
    return count


def iter_jsonl_gz(store: ObjectStore, key: str) -> Iterator[JSON]:
    """Yield the records of a gzipped JSON Lines object."""
    text = gzip.decompress(store.read_bytes(key)).decode("utf-8")
    for line in text.splitlines():
        if line.strip():
            yield json.loads(line)


def write_json(store: ObjectStore, key: str, payload: Any) -> None:
    """Write a small JSON document (state, manifests, metadata)."""
    store.write_bytes(key, json.dumps(payload, indent=2, sort_keys=True, default=str).encode())


def read_json(store: ObjectStore, key: str) -> Any:
    return json.loads(store.read_bytes(key))

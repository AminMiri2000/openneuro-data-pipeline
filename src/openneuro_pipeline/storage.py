"""Raw-layer storage and pipeline state.

Every write goes to a temporary file first and is then renamed into place, so a
crash mid-run never leaves a half-written file that a later step would load.
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise


def write_jsonl_gz(path: Path, records: Iterable[dict[str, Any]]) -> int:
    """Write records as gzipped JSON Lines, atomically. Returns the record count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    count = 0
    try:
        with os.fdopen(fd, "wb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            for record in records:
                gz.write(json.dumps(record, sort_keys=True, ensure_ascii=False).encode() + b"\n")
                count += 1
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return count


def read_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh if line.strip()]


class StateStore:
    """Tiny JSON key-value store for incremental bookkeeping (watermarks, seen keys)."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def save(self, state: dict[str, Any]) -> None:
        payload = json.dumps(state, indent=2, sort_keys=True).encode()
        atomic_write_bytes(self.path, payload)

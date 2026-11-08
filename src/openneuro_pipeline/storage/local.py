"""Local-disk implementation of `ObjectStore`."""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from ..errors import StorageError
from .base import ObjectInfo

# Temporary files start with this prefix and are never listed as objects.
_TMP_PREFIX = "."


class LocalObjectStore:
    """Stores objects as files under a root directory.

    Writes go to a hidden temporary file in the target folder and are then
    renamed into place. `os.replace` is atomic on the same filesystem, so a
    reader (or a crash) never sees a half-written object. This mirrors S3,
    where a PUT is all-or-nothing.
    """

    def __init__(self, root: Path) -> None:
        # Resolve once, up front: every key is computed relative to an absolute
        # root, so a relative setting such as "data/raw" behaves the same as an
        # absolute one regardless of how paths are later resolved.
        self.root = Path(root).resolve()

    # ------------------------------------------------------------------ writing

    def write_bytes(self, key: str, data: bytes) -> None:
        with self.open_writer(key) as fh:
            fh.write(data)

    @contextmanager
    def open_writer(self, key: str) -> Iterator[BinaryIO]:
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            dir=target.parent, prefix=f"{_TMP_PREFIX}{target.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "wb") as fh:
                yield fh
            os.replace(tmp_name, target)
        except BaseException:
            # Any failure (including Ctrl-C) discards the partial file.
            Path(tmp_name).unlink(missing_ok=True)
            raise

    # ------------------------------------------------------------------ reading

    def read_bytes(self, key: str) -> bytes:
        try:
            return self._path(key).read_bytes()
        except FileNotFoundError as exc:
            raise StorageError(f"Object not found: {key}") from exc

    def exists(self, key: str) -> bool:
        return self._path(key).is_file()

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        base = self._path(prefix) if prefix else self.root
        if not base.exists():
            return
        for path in sorted(base.rglob("*")):
            if path.is_file() and not path.name.startswith(_TMP_PREFIX):
                yield path.relative_to(self.root).as_posix()

    def stat(self, key: str) -> ObjectInfo:
        try:
            st = self._path(key).stat()
        except FileNotFoundError as exc:
            raise StorageError(f"Object not found: {key}") from exc
        return ObjectInfo(key=key, size=st.st_size, fingerprint=f"{st.st_size}-{st.st_mtime_ns}")

    def uri(self, key: str) -> str:
        return str(self._path(key).resolve())

    # ------------------------------------------------------------------ helpers

    def _path(self, key: str) -> Path:
        """Resolve a key to a path, refusing keys that escape the root."""
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root):
            raise StorageError(f"Key escapes the store root: {key}")
        return path

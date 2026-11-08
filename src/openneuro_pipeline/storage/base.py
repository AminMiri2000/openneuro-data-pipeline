"""Object-store abstraction for the raw layer.

The rest of the pipeline only sees this interface, never the filesystem. Today
the implementation is the local disk (free); moving the raw layer to S3, GCS or
MinIO means adding one class that implements `ObjectStore`, with no change to
the extractors or the loader.

Keys are relative, '/'-separated paths such as
`openneuro/datasets/ingest_date=2026-11-07/modality=eeg/datasets.jsonl.gz`.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import BinaryIO, Protocol, runtime_checkable


@dataclass(frozen=True)
class ObjectInfo:
    """Metadata about a stored object.

    `fingerprint` changes whenever the object's content is replaced (an ETag on
    S3, size + modification time on disk). The loader compares fingerprints to
    decide whether a partition needs reloading.
    """

    key: str
    size: int
    fingerprint: str


@runtime_checkable
class ObjectStore(Protocol):
    """Minimal storage interface the pipeline depends on."""

    def write_bytes(self, key: str, data: bytes) -> None:
        """Store `data` under `key`, replacing any existing object atomically."""
        ...

    def open_writer(self, key: str) -> AbstractContextManager[BinaryIO]:
        """Stream bytes to `key`; the object becomes visible only on clean exit."""
        ...

    def read_bytes(self, key: str) -> bytes:
        """Return the object's content; raise StorageError if it is missing."""
        ...

    def exists(self, key: str) -> bool:
        """Whether an object exists at `key`."""
        ...

    def list_keys(self, prefix: str = "") -> Iterator[str]:
        """Yield every key under `prefix`, in sorted order."""
        ...

    def stat(self, key: str) -> ObjectInfo:
        """Return metadata for `key`; raise StorageError if it is missing."""
        ...

    def uri(self, key: str) -> str:
        """Location of `key` that DuckDB can read directly (path or s3:// URI)."""
        ...

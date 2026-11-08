"""HTTP client for OpenNeuro.

Responsibilities are limited to talking to OpenNeuro: retries, pagination and
error translation. Where the data lands is the extractors' job.

Thread safety: `requests.Session` is not guaranteed to be thread-safe, so each
thread lazily gets its own session (with its own connection pool). This lets
extractors fan out downloads over a thread pool without sharing sockets.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ...config import Settings
from ...errors import SourceError
from .queries import DATASETS_QUERY

log = logging.getLogger(__name__)

JSON = dict[str, Any]

# Status codes worth retrying: rate limiting and transient server failures.
RETRYABLE_STATUSES = (429, 500, 502, 503, 504)
# A public S3 bucket answers 403 (not 404) for a key that does not exist.
MISSING_OBJECT_STATUSES = (403, 404)


@dataclass
class PageStats:
    """Counters collected while paginating, reported in the run manifest."""

    pages: int = 0
    nodes: int = 0
    null_nodes: int = 0
    graphql_errors: list[str] = field(default_factory=list)


def build_session(settings: Settings, max_retries: int = 5) -> requests.Session:
    """Create a session that retries transient failures with exponential backoff."""
    retry = Retry(
        total=max_retries,
        backoff_factor=1.0,  # sleeps 1s, 2s, 4s, ... between attempts
        status_forcelist=RETRYABLE_STATUSES,
        # POST is safe to retry here: our GraphQL calls are read-only queries.
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,  # we inspect the final status ourselves
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=settings.max_workers)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = settings.user_agent
    return session


class OpenNeuroClient:
    """Read-only client for the OpenNeuro GraphQL API and public S3 bucket."""

    def __init__(self, settings: Settings, max_retries: int = 5) -> None:
        self._settings = settings
        self._max_retries = max_retries
        self._local = threading.local()

    @property
    def session(self) -> requests.Session:
        """The calling thread's session, created on first use."""
        session: requests.Session | None = getattr(self._local, "session", None)
        if session is None:
            session = build_session(self._settings, self._max_retries)
            self._local.session = session
        return session

    # ------------------------------------------------------------------ GraphQL

    def graphql(self, query: str, variables: JSON | None = None) -> JSON:
        """Execute a GraphQL query and return the full response body.

        Partial success (both `data` and `errors`) is returned as-is so the
        caller can decide what to keep; errors with no data raise SourceError.
        """
        resp = self.session.post(
            self._settings.graphql_url,
            json={"query": query, "variables": variables or {}},
            timeout=self._settings.request_timeout,
        )
        if resp.status_code >= 400:
            raise SourceError(f"GraphQL HTTP {resp.status_code}: {resp.text[:300]}")
        try:
            body: JSON = resp.json()
        except ValueError as exc:
            raise SourceError(f"GraphQL returned non-JSON: {resp.text[:300]}") from exc

        errors = body.get("errors") or []
        if errors and not body.get("data"):
            messages = "; ".join(str(e.get("message", e)) for e in errors)
            raise SourceError(f"GraphQL errors: {messages}")
        return body

    def iter_datasets(
        self,
        modality: str | None = None,
        limit: int | None = None,
        stats: PageStats | None = None,
    ) -> Iterator[JSON]:
        """Yield dataset nodes, following Relay-style cursors page by page.

        A generator keeps memory flat however large the catalogue grows: only
        one page is held at a time.

        Args:
            modality: OpenNeuro modality filter, or None for every dataset.
            limit: Stop after this many nodes (useful for trial runs).
            stats: Optional counters to fill in while paginating.
        """
        stats = stats if stats is not None else PageStats()
        cursor: str | None = None
        while True:
            body = self.graphql(
                DATASETS_QUERY,
                {"first": self._settings.page_size, "after": cursor, "modality": modality},
            )
            stats.pages += 1
            stats.graphql_errors.extend(str(e.get("message", e)) for e in body.get("errors") or [])

            connection = (body.get("data") or {}).get("datasets")
            if connection is None:
                raise SourceError("GraphQL response has no 'datasets' field")

            for edge in connection.get("edges") or []:
                node = (edge or {}).get("node")
                if not node:
                    # OpenNeuro sometimes nulls a node it cannot resolve; count it, move on.
                    stats.null_nodes += 1
                    continue
                stats.nodes += 1
                yield node
                if limit is not None and stats.nodes >= limit:
                    return

            cursor = self._next_cursor(connection.get("pageInfo") or {}, previous=cursor)
            if cursor is None:
                return

    @staticmethod
    def _next_cursor(page_info: JSON, previous: str | None) -> str | None:
        """Return the next page's cursor, or None when the last page was read."""
        if not page_info.get("hasNextPage"):
            return None
        cursor = page_info.get("endCursor")
        # Guard against an API bug turning pagination into an infinite loop.
        if not cursor or cursor == previous:
            raise SourceError("Pagination cursor did not advance; aborting")
        return str(cursor)

    # ----------------------------------------------------------------------- S3

    def fetch_dataset_file(self, dataset_id: str, filename: str) -> bytes | None:
        """Download a top-level dataset file; return None if it does not exist."""
        url = f"{self._settings.s3_base_url}/{dataset_id}/{filename}"
        resp = self.session.get(url, timeout=self._settings.request_timeout)
        if resp.status_code in MISSING_OBJECT_STATUSES:
            return None
        if resp.status_code >= 400:
            raise SourceError(f"S3 HTTP {resp.status_code} for {url}")
        return resp.content

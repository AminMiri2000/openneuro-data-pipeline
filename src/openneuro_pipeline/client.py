"""HTTP clients for OpenNeuro: the GraphQL API and the public S3 bucket.

Design notes
------------
* Retries with exponential backoff on 429 and 5xx, so a flaky public API does
  not fail the whole run.
* GraphQL "partial success" (data plus errors) is tolerated and reported;
  a response with errors and no data is a hard failure.
* Pagination is cursor-based (Relay style), exposed as a generator so callers
  never hold the full catalogue in memory.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import Settings

log = logging.getLogger(__name__)

DATASETS_QUERY = """
query Datasets($first: Int!, $after: String, $modality: String) {
  datasets(first: $first, after: $after, modality: $modality) {
    edges {
      node {
        id
        created
        latestSnapshot {
          id
          tag
          created
          description {
            Name
            Authors
            License
            DatasetDOI
            BIDSVersion
          }
          summary {
            modalities
            subjects
            sessions
            tasks
            size
            totalFiles
          }
        }
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


class OpenNeuroError(RuntimeError):
    """Raised when OpenNeuro returns an unusable response."""


@dataclass
class PageStats:
    pages: int = 0
    nodes: int = 0
    null_nodes: int = 0
    graphql_errors: list[str] = field(default_factory=list)


def build_session(settings: Settings) -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.0,  # 1s, 2s, 4s, 8s, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": settings.user_agent})
    return session


class OpenNeuroClient:
    def __init__(self, settings: Settings, session: requests.Session | None = None) -> None:
        self.settings = settings
        self.session = session or build_session(settings)

    # ---------------------------------------------------------------- GraphQL
    def graphql(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        resp = self.session.post(
            self.settings.graphql_url,
            json={"query": query, "variables": variables or {}},
            timeout=self.settings.request_timeout,
        )
        if resp.status_code >= 400:
            raise OpenNeuroError(f"GraphQL HTTP {resp.status_code}: {resp.text[:300]}")
        try:
            body = resp.json()
        except ValueError as exc:
            raise OpenNeuroError(f"GraphQL returned non-JSON: {resp.text[:300]}") from exc

        errors = body.get("errors") or []
        if errors and not body.get("data"):
            messages = "; ".join(e.get("message", str(e)) for e in errors)
            raise OpenNeuroError(f"GraphQL errors: {messages}")
        return body

    def iter_datasets(
        self,
        modality: str | None = None,
        limit: int | None = None,
        stats: PageStats | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield dataset nodes page by page until the catalogue (or `limit`) is exhausted."""
        stats = stats if stats is not None else PageStats()
        after: str | None = None
        while True:
            body = self.graphql(
                DATASETS_QUERY,
                {"first": self.settings.page_size, "after": after, "modality": modality},
            )
            stats.pages += 1
            for err in body.get("errors") or []:
                stats.graphql_errors.append(err.get("message", str(err)))

            connection = (body.get("data") or {}).get("datasets")
            if connection is None:
                raise OpenNeuroError("GraphQL response has no 'datasets' field")

            for edge in connection.get("edges") or []:
                node = (edge or {}).get("node")
                if not node:
                    # OpenNeuro occasionally nulls a node it cannot resolve.
                    stats.null_nodes += 1
                    continue
                stats.nodes += 1
                yield node
                if limit is not None and stats.nodes >= limit:
                    return

            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                return
            next_cursor = page_info.get("endCursor")
            if not next_cursor or next_cursor == after:
                raise OpenNeuroError("Pagination cursor did not advance; aborting to avoid a loop")
            after = next_cursor

    # --------------------------------------------------------------------- S3
    def fetch_dataset_file(self, dataset_id: str, filename: str) -> bytes | None:
        """Download a top-level file of a dataset; None when it does not exist."""
        url = f"{self.settings.s3_base_url}/{dataset_id}/{filename}"
        resp = self.session.get(url, timeout=self.settings.request_timeout)
        if resp.status_code in (403, 404):  # S3 answers 403 for missing keys on public buckets
            return None
        if resp.status_code >= 400:
            raise OpenNeuroError(f"S3 HTTP {resp.status_code} for {url}")
        return resp.content

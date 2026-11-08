"""Runtime configuration.

All settings come from environment variables (twelve-factor style) with
defaults that run the whole pipeline locally for free. The object is frozen and
validated on creation, so a bad value fails fast at start-up rather than
half-way through a run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from .errors import ConfigError

DEFAULT_GRAPHQL_URL = "https://openneuro.org/crn/graphql"
# Public, anonymous HTTPS endpoint of OpenNeuro's S3 bucket: no AWS account needed.
DEFAULT_S3_BASE_URL = "https://s3.amazonaws.com/openneuro.org"


@dataclass(frozen=True)
class Settings:
    """Typed, validated pipeline settings.

    Attributes:
        graphql_url: OpenNeuro GraphQL endpoint.
        s3_base_url: Base URL for downloading individual dataset files.
        data_dir: Root folder for raw files, state and the DuckDB warehouse.
        page_size: Datasets requested per GraphQL page.
        request_timeout: Per-request timeout in seconds.
        max_workers: Parallel downloads for per-dataset fetches. Kept small by
            default to stay polite to a free public service.
        insert_batch_size: Rows per INSERT batch when loading the warehouse,
            which bounds memory use regardless of data volume.
        user_agent: Identifies this client to OpenNeuro.
    """

    graphql_url: str = DEFAULT_GRAPHQL_URL
    s3_base_url: str = DEFAULT_S3_BASE_URL
    data_dir: Path = Path("data")
    page_size: int = 50
    request_timeout: float = 60.0
    max_workers: int = 4
    insert_batch_size: int = 5_000
    user_agent: str = "openneuro-data-pipeline/0.2 (portfolio project)"

    def __post_init__(self) -> None:
        if not 1 <= self.page_size <= 500:
            raise ConfigError(f"page_size must be 1-500, got {self.page_size}")
        if self.request_timeout <= 0:
            raise ConfigError(f"request_timeout must be positive, got {self.request_timeout}")
        if not 1 <= self.max_workers <= 32:
            raise ConfigError(f"max_workers must be 1-32, got {self.max_workers}")
        if self.insert_batch_size < 1:
            raise ConfigError(f"insert_batch_size must be >= 1, got {self.insert_batch_size}")

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment variables, falling back to defaults."""
        env = os.environ
        try:
            return cls(
                graphql_url=env.get("OPENNEURO_GRAPHQL_URL", DEFAULT_GRAPHQL_URL),
                s3_base_url=env.get("OPENNEURO_S3_BASE_URL", DEFAULT_S3_BASE_URL),
                data_dir=Path(env.get("PIPELINE_DATA_DIR", "data")).expanduser(),
                page_size=int(env.get("OPENNEURO_PAGE_SIZE", "50")),
                request_timeout=float(env.get("OPENNEURO_TIMEOUT", "60")),
                max_workers=int(env.get("PIPELINE_MAX_WORKERS", "4")),
                insert_batch_size=int(env.get("PIPELINE_INSERT_BATCH_SIZE", "5000")),
            )
        except ValueError as exc:  # e.g. OPENNEURO_PAGE_SIZE=abc
            raise ConfigError(f"Invalid numeric environment variable: {exc}") from exc

    @property
    def raw_dir(self) -> Path:
        """Root of the raw (landing) layer."""
        return self.data_dir / "raw"

    @property
    def warehouse_path(self) -> Path:
        """Path of the DuckDB warehouse file."""
        return self.data_dir / "warehouse.duckdb"

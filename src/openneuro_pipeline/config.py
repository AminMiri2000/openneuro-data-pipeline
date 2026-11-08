"""Runtime settings, read from environment variables with sensible local defaults.

Everything runs locally and for free: raw files live on disk, the warehouse is a
single DuckDB file. Override any setting with an environment variable.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env_path(name: str, default: str) -> Path:
    return Path(os.environ.get(name, default)).expanduser()


@dataclass(frozen=True)
class Settings:
    graphql_url: str = field(
        default_factory=lambda: os.environ.get(
            "OPENNEURO_GRAPHQL_URL", "https://openneuro.org/crn/graphql"
        )
    )
    # Public, anonymous HTTPS endpoint of the OpenNeuro S3 bucket (no AWS account needed).
    s3_base_url: str = field(
        default_factory=lambda: os.environ.get(
            "OPENNEURO_S3_BASE_URL", "https://s3.amazonaws.com/openneuro.org"
        )
    )
    data_dir: Path = field(default_factory=lambda: _env_path("PIPELINE_DATA_DIR", "data"))
    page_size: int = field(default_factory=lambda: int(os.environ.get("OPENNEURO_PAGE_SIZE", "50")))
    request_timeout: float = field(
        default_factory=lambda: float(os.environ.get("OPENNEURO_TIMEOUT", "60"))
    )
    # Be polite to a free public service.
    user_agent: str = "openneuro-data-pipeline/0.1 (portfolio project)"

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw" / "openneuro"

    @property
    def state_dir(self) -> Path:
        return self.data_dir / "state"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"


def get_settings() -> Settings:
    return Settings()

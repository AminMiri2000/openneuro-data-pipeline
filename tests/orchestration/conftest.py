"""Fixtures shared by the Dagster tests."""

import pytest

from openneuro_pipeline.config import Settings
from openneuro_pipeline.orchestration.builder import build_definitions
from tests.conftest import DBT_PROJECT_DIR, GRAPHQL_URL, S3_URL


@pytest.fixture
def env_settings(tmp_path, monkeypatch):
    """Point everything (CLI-style env vars) at a temporary data directory."""
    data_dir = tmp_path / "data"
    for name, value in {
        "PIPELINE_DATA_DIR": str(data_dir),
        "OPENNEURO_GRAPHQL_URL": GRAPHQL_URL,
        "OPENNEURO_S3_BASE_URL": S3_URL,
        "OPENNEURO_PAGE_SIZE": "2",
        "PIPELINE_DBT_PROJECT_DIR": str(DBT_PROJECT_DIR),
        # Set (and later restored) here because building the dbt assets sets it.
        "PIPELINE_WAREHOUSE_PATH": str(data_dir / "warehouse.duckdb"),
    }.items():
        monkeypatch.setenv(name, value)
    return Settings.from_env()


@pytest.fixture
def defs(env_settings):
    return build_definitions(env_settings)

"""Dagster definitions: graph wiring, schedule, instance and a full run."""

import json

import pytest
from dagster import AssetKey, DagsterInstance

from openneuro_pipeline.config import Settings
from openneuro_pipeline.orchestration.builder import DAILY_CRON, build_definitions
from openneuro_pipeline.orchestration.instance import INSTANCE_CONFIG, prepare_instance
from tests.conftest import DBT_PROJECT_DIR, GRAPHQL_URL, S3_URL

CATALOGUE = AssetKey(["raw_files", "openneuro_catalogue"])
RAW_DATASETS = AssetKey(["openneuro", "openneuro_datasets"])
DIM_DATASET = AssetKey(["marts", "dim_dataset"])
FCT_PARTICIPANT = AssetKey(["marts", "fct_participant"])


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


def test_lineage_runs_from_api_to_marts(defs):
    graph = defs.resolve_asset_graph()

    def ancestors(key):
        seen, todo = set(), [key]
        while todo:
            for parent in graph.get(todo.pop()).parent_keys:
                if parent not in seen:
                    seen.add(parent)
                    todo.append(parent)
        return seen

    # The load step's keys must equal the dbt source keys, or the graph breaks
    # into two disconnected halves.
    assert {CATALOGUE, RAW_DATASETS} <= ancestors(DIM_DATASET)
    assert graph.get(RAW_DATASETS).is_materializable


def test_dbt_tests_become_asset_checks(defs):
    check_names = {key.name for key in defs.resolve_asset_graph().asset_check_keys}
    assert "no_failed_downloads" in check_names
    assert any(name.startswith("unique_dim_dataset") for name in check_names)


def test_daily_schedule_is_off_by_default(defs):
    schedule = defs.resolve_schedule_def("daily_refresh_schedule")
    assert schedule.cron_schedule == DAILY_CRON
    assert schedule.execution_timezone == "America/Toronto"
    assert schedule.default_status.value == "STOPPED"


def test_instance_runs_one_job_at_a_time(env_settings):
    home = prepare_instance(env_settings)
    config = json.loads((home / "dagster.yaml").read_text())
    assert config == INSTANCE_CONFIG
    assert config["run_queue"]["max_concurrent_runs"] == 1


def test_full_materialisation(defs, mocked_api):
    """The scheduled job, end to end: every asset and asset check."""
    job = defs.resolve_job_def("daily_refresh")
    result = job.execute_in_process(instance=DagsterInstance.ephemeral())
    assert result.success

    materialised = {event.asset_key for event in result.get_asset_materialization_events()}
    assert {CATALOGUE, RAW_DATASETS, DIM_DATASET, FCT_PARTICIPANT} <= materialised

    failed_checks = [
        evaluation.check_name
        for evaluation in result.get_asset_check_evaluations()
        if not evaluation.passed
    ]
    assert failed_checks == []

    raw_meta = next(
        event.materialization.metadata
        for event in result.get_asset_materialization_events()
        if event.asset_key == RAW_DATASETS
    )
    assert raw_meta["rows_inserted"].value == 4

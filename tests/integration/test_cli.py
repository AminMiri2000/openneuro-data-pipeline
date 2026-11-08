import pytest
from click.testing import CliRunner

from openneuro_pipeline.cli import EXIT_ERROR, main
from tests.conftest import DBT_PROJECT_DIR, GRAPHQL_URL, S3_URL, json_from_output


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("OPENNEURO_GRAPHQL_URL", GRAPHQL_URL)
    monkeypatch.setenv("OPENNEURO_S3_BASE_URL", S3_URL)
    monkeypatch.setenv("OPENNEURO_PAGE_SIZE", "2")
    monkeypatch.setenv("PIPELINE_DBT_PROJECT_DIR", str(DBT_PROJECT_DIR))
    return tmp_path / "data"


def test_run_end_to_end(env, mocked_api):
    result = CliRunner().invoke(main, ["run", "--ingest-date", "2026-11-07"])

    assert result.exit_code == 0, result.output
    summary = json_from_output(result.output)
    datasets, participants = summary["extracts"]
    assert datasets["metrics"]["records"] == 4
    assert participants["metrics"]["fetched"] == 2
    assert summary["load"]["tables"]["raw.openneuro_participants"]["rows_inserted"] == 4
    assert summary["transform"]["status_counts"]["success"] >= 7  # 3 seeds + 7 models
    assert "error" not in summary["transform"]["status_counts"]
    assert (env / "warehouse.duckdb").exists()


def test_extract_only_participants_pulls_in_datasets(env, mocked_api):
    result = CliRunner().invoke(main, ["extract", "participants"])
    assert result.exit_code == 0, result.output
    assert [r["step"] for r in json_from_output(result.output)] == [
        "datasets",
        "participants",
    ]


def test_config_error_gives_clean_exit(env, monkeypatch):
    monkeypatch.setenv("PIPELINE_MAX_WORKERS", "0")
    result = CliRunner().invoke(main, ["load"])
    assert result.exit_code == EXIT_ERROR


def test_query_reads_warehouse(env, mocked_api):
    runner = CliRunner()
    assert runner.invoke(main, ["run", "--skip-transform"]).exit_code == 0
    result = runner.invoke(main, ["query", "SELECT count(*) AS n FROM raw.openneuro_datasets"])
    assert result.exit_code == 0, result.output
    assert "4" in result.output


def test_query_cannot_modify_data(env, mocked_api):
    runner = CliRunner()
    runner.invoke(main, ["run", "--skip-transform"])
    result = runner.invoke(main, ["query", "DELETE FROM raw.openneuro_datasets"])
    assert result.exit_code == EXIT_ERROR  # read-only connection rejects writes
    assert "query error" in result.output


def test_transform_and_dbt_passthrough(env, mocked_api):
    runner = CliRunner()
    assert runner.invoke(main, ["run", "--skip-transform"]).exit_code == 0

    built = runner.invoke(main, ["transform", "--select", "staging"])
    assert built.exit_code == 0, built.output
    assert json_from_output(built.output)["command"] == "build --select staging"

    listed = runner.invoke(main, ["dbt", "ls", "--resource-type", "model", "--output", "name"])
    assert listed.exit_code == 0, listed.output
    assert "dim_dataset" in listed.output


def test_transform_failure_gives_clean_exit(env):
    # No raw tables yet, so the staging models cannot build.
    result = CliRunner().invoke(main, ["transform"])
    assert result.exit_code == EXIT_ERROR

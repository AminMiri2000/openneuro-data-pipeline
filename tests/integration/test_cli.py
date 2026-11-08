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


def test_dagster_command_prepares_instance_and_launches(env, monkeypatch):
    launched = {}

    class FakeProcess:
        def __init__(self, command, env):
            launched["command"], launched["env"] = command, env

        def wait(self):
            return 0

    monkeypatch.setattr("openneuro_pipeline.cli.subprocess.Popen", FakeProcess)
    result = CliRunner().invoke(main, ["dagster", "--port", "3333"])

    assert result.exit_code == 0, result.output
    command = launched["command"]
    assert command[1:4] == ["-m", "dagster", "dev"]
    assert "openneuro_pipeline.orchestration.definitions" in command
    assert command[-2:] == ["--port", "3333"]
    home = launched["env"]["DAGSTER_HOME"]
    assert (env / "dagster" / "dagster.yaml").is_file() and home.endswith("dagster")


@pytest.fixture
def captured_alerts(monkeypatch):
    sent = []
    monkeypatch.setattr("openneuro_pipeline.cli.send_digest", lambda d, s: sent.append(d) or True)
    for name in ("ALERT_SMTP_USER", "ALERT_SMTP_PASSWORD", "GITHUB_RUN_ID"):
        monkeypatch.delenv(name, raising=False)
    return sent


def test_notify_sends_digest_for_failed_items(env, captured_alerts, monkeypatch):
    import responses as rsps_lib

    from tests.conftest import register_catalogue

    monkeypatch.setenv("ALERT_SOURCE_NAME", "Test refresh")
    with rsps_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        register_catalogue(rsps)
        for ds in ("ds000001", "ds000002", "ds000003"):
            rsps.add(rsps_lib.GET, f"{S3_URL}/{ds}/participants.tsv", status=404)
        rsps.replace(rsps_lib.GET, f"{S3_URL}/ds000001/participants.tsv", status=500)
        monkeypatch.setenv("OPENNEURO_TIMEOUT", "5")
        result = CliRunner().invoke(main, ["run", "--skip-transform", "--notify"])

    assert result.exit_code == 1  # partial failure
    [digest] = captured_alerts
    assert digest.source == "Test refresh"
    assert "failed in 'participants'" in digest.issues[0].title


def test_notify_stays_quiet_on_clean_run(env, mocked_api, captured_alerts):
    result = CliRunner().invoke(main, ["run", "--notify"])
    assert result.exit_code == 0, result.output
    assert captured_alerts == []


def test_notify_reports_a_failed_run(env, captured_alerts, monkeypatch):
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    import responses as rsps_lib

    # The API answers with a GraphQL error and no data: the run stops at once.
    with rsps_lib.RequestsMock() as rsps:
        rsps.add(rsps_lib.POST, GRAPHQL_URL, json={"errors": [{"message": "schema changed"}]})
        result = CliRunner().invoke(main, ["run", "--notify"])

    assert result.exit_code != 0
    [digest] = captured_alerts
    assert digest.has_failures
    assert digest.link == "https://github.com/me/repo/actions/runs/42"


def test_notify_test_requires_configuration(env, captured_alerts):
    result = CliRunner().invoke(main, ["notify-test"])
    assert result.exit_code == EXIT_ERROR


def test_dashboard_command(env, mocked_api):
    runner = CliRunner()
    assert runner.invoke(main, ["run"]).exit_code == 0
    result = runner.invoke(main, ["dashboard", "--output", str(env / "site")])
    assert result.exit_code == 0, result.output
    assert (env / "site" / "index.html").is_file()
    assert "4 datasets" in result.output


def test_notify_does_not_repeat_an_ongoing_warning(env, captured_alerts, monkeypatch):
    import responses as rsps_lib

    from tests.conftest import register_catalogue

    def run_with_failing_download():
        with rsps_lib.RequestsMock(assert_all_requests_are_fired=False) as rsps:
            register_catalogue(rsps)
            for ds in ("ds000002", "ds000003"):
                rsps.add(rsps_lib.GET, f"{S3_URL}/{ds}/participants.tsv", status=404)
            rsps.add(rsps_lib.GET, f"{S3_URL}/ds000001/participants.tsv", status=500)
            return CliRunner().invoke(main, ["run", "--skip-transform", "--notify"])

    run_with_failing_download()
    run_with_failing_download()  # same dataset still failing the next day
    assert len(captured_alerts) == 1

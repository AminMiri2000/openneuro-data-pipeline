"""Alert sensors, evaluated against real in-process runs."""

from datetime import timedelta

import pytest
import responses
from dagster import (
    AssetKey,
    DagsterInstance,
    DagsterRunStatus,
    build_run_status_sensor_context,
    build_sensor_context,
)

from openneuro_pipeline.orchestration import sensors
from tests.conftest import S3_URL, register_catalogue


@pytest.fixture
def sent(monkeypatch):
    digests = []
    monkeypatch.setattr(sensors, "send_digest", lambda d, s: digests.append(d) or True)
    return digests


@pytest.fixture
def instance():
    with DagsterInstance.ephemeral() as ephemeral:
        yield ephemeral


def _status_context(instance, result, status):
    run = instance.get_run_by_id(result.run_id)
    event = next(e for e in result.all_events if e.event_type_value == status_event_type(status))
    return build_run_status_sensor_context(
        sensor_name="test", dagster_instance=instance, dagster_run=run, dagster_event=event
    )


def status_event_type(status):
    return {
        DagsterRunStatus.SUCCESS: "PIPELINE_SUCCESS",
        DagsterRunStatus.FAILURE: "PIPELINE_FAILURE",
    }[status]


def test_check_warnings_digest_lists_failed_checks(defs, instance, sent):
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        register_catalogue(rsps)
        for ds in ("ds000002", "ds000003"):
            rsps.add(responses.GET, f"{S3_URL}/{ds}/participants.tsv", status=404)
        # ds000001 keeps failing: the warn-level download check fails, the run succeeds.
        rsps.add(responses.GET, f"{S3_URL}/ds000001/participants.tsv", status=500)
        result = defs.resolve_job_def("daily_refresh").execute_in_process(instance=instance)
    assert result.success

    sensors.email_on_check_warnings(_status_context(instance, result, DagsterRunStatus.SUCCESS))

    [digest] = sent
    assert "raw_files/openneuro_participant_files: no_failed_downloads" in digest.issues[0].details
    assert digest.link.endswith(result.run_id)


def test_clean_run_sends_nothing(defs, instance, sent, mocked_api):
    result = defs.resolve_job_def("daily_refresh").execute_in_process(instance=instance)
    sensors.email_on_check_warnings(_status_context(instance, result, DagsterRunStatus.SUCCESS))
    assert sent == []


def test_failed_run_sends_failure_digest(defs, instance, sent):
    # dbt on an empty warehouse fails fast (no raw tables, and dbt assets have
    # no retry policy), which makes a quick, deterministic failing run.
    job = defs.resolve_implicit_global_asset_job_def()
    result = job.execute_in_process(
        instance=instance,
        asset_selection=[AssetKey(["staging", "stg_openneuro__datasets"])],
        raise_on_error=False,
    )
    assert not result.success

    context = _status_context(instance, result, DagsterRunStatus.FAILURE).for_run_failure()
    sensors.email_on_run_failure(context)

    [digest] = sent
    assert digest.has_failures and digest.source.startswith("Dagster ")


def test_stale_data_alerts_once_per_day(defs, instance, sent, mocked_api, monkeypatch):
    context = build_sensor_context(instance=instance)
    assert "never been materialised" in sensors.email_on_stale_data(context).skip_message

    defs.resolve_job_def("daily_refresh").execute_in_process(instance=instance)
    assert "fresh enough" in sensors.email_on_stale_data(context).skip_message

    monkeypatch.setattr(sensors, "STALE_AFTER", timedelta(seconds=-1))
    sensors.email_on_stale_data(context)
    sensors.email_on_stale_data(context)  # same day: no second email
    assert len(sent) == 1 and "last refreshed" in sent[0].issues[0].title

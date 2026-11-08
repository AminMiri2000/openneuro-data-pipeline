"""Alert sensors: email when a run fails, when checks warn, or when data goes stale.

Sensors are evaluated by the Dagster daemon, which `openneuro-pipeline
dagster` starts. Email settings come from the ALERT_* environment variables
(a `.env` file in the repository root is loaded automatically). Without
them, digests are written to the Dagster logs instead of sent.
"""

import os
from datetime import UTC, datetime, timedelta

from dagster import (
    AssetCheckEvaluation,
    DagsterEventType,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunFailureSensorContext,
    RunStatusSensorContext,
    SensorEvaluationContext,
    SkipReason,
    run_failure_sensor,
    run_status_sensor,
    sensor,
)

from ..alerts import AlertMemory, Digest, EmailSettings, Issue, Severity, send_digest
from ..config import Settings
from ..storage import LocalObjectStore
from .assets import CATALOGUE_KEY

SOURCE = "Dagster"
# The schedule runs daily; two hours of slack covers a slow or retried run.
STALE_AFTER = timedelta(hours=26)


def _run_link(run_id: str) -> str:
    base = os.environ.get("DAGSTER_UI_URL", "http://localhost:3000").rstrip("/")
    return f"{base}/runs/{run_id}"


def failed_check_issues(context: RunStatusSensorContext) -> list[Issue]:
    """Asset checks (including dbt tests) that did not pass in this run."""
    details = []
    for entry in context.instance.all_logs(
        context.dagster_run.run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION
    ):
        event = entry.dagster_event
        evaluation = event.event_specific_data if event else None
        if isinstance(evaluation, AssetCheckEvaluation) and not evaluation.passed:
            # "check (asset)": no ": ", so the whole line identifies the warning.
            details.append(f"{evaluation.check_name} ({evaluation.asset_key.to_user_string()})")
    if not details:
        return []
    return [Issue(Severity.WARNING, f"{len(details)} check(s) did not pass", tuple(details))]


@run_failure_sensor(
    name="email_on_run_failure",
    default_status=DefaultSensorStatus.RUNNING,
    description="Email when any run in this project fails.",
)
def email_on_run_failure(context: RunFailureSensorContext) -> None:
    run = context.dagster_run
    message = context.failure_event.message or "unknown error"
    digest = Digest(
        source=f"{SOURCE} {run.job_name}",
        issues=[Issue(Severity.FAILURE, "The run failed", (message,))],
        link=_run_link(run.run_id),
    )
    send_digest(digest, EmailSettings.from_env())


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    name="email_on_check_warnings",
    default_status=DefaultSensorStatus.RUNNING,
    description="After a successful run, email a digest of checks that warned.",
)
def email_on_check_warnings(context: RunStatusSensorContext) -> None:
    run = context.dagster_run
    digest = Digest(
        source=f"{SOURCE} {run.job_name}",
        issues=failed_check_issues(context),
        link=_run_link(run.run_id),
    )
    # Report each warning once, not on every run while it persists.
    digest = AlertMemory(LocalObjectStore(Settings.from_env().raw_dir)).only_new(digest)
    if digest.issues:
        send_digest(digest, EmailSettings.from_env())


@sensor(
    name="email_on_stale_data",
    minimum_interval_seconds=3600,
    # Off by default, like the schedule: switch both on together, otherwise a
    # paused schedule would trigger a stale-data email every day.
    default_status=DefaultSensorStatus.STOPPED,
    description=f"Email once a day if the catalogue is older than {STALE_AFTER}.",
)
def email_on_stale_data(context: SensorEvaluationContext) -> SkipReason:
    event = context.instance.get_latest_materialization_event(CATALOGUE_KEY)
    if event is None:
        return SkipReason("The catalogue has never been materialised.")
    age = datetime.now(UTC) - datetime.fromtimestamp(event.timestamp, UTC)
    if age <= STALE_AFTER:
        return SkipReason(f"Catalogue is {age} old; fresh enough.")

    today = datetime.now(UTC).date().isoformat()
    if context.cursor == today:  # already alerted today
        return SkipReason("Stale-data alert already sent today.")
    send_digest(
        Digest(
            source=f"{SOURCE} freshness",
            issues=[
                Issue(
                    Severity.WARNING,
                    f"The catalogue was last refreshed {age.days} day(s) "
                    f"{age.seconds // 3600} hour(s) ago",
                    ("Check that the daily_refresh_schedule is on and its last run succeeded.",),
                )
            ],
            link=os.environ.get("DAGSTER_UI_URL", "http://localhost:3000"),
        ),
        EmailSettings.from_env(),
    )
    context.update_cursor(today)
    return SkipReason("Stale-data alert sent.")

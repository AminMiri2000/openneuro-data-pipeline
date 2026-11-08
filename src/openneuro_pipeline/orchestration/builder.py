"""Assemble all assets, the daily job and its schedule into Dagster definitions."""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from ..config import Settings
from .assets import openneuro_catalogue, openneuro_participant_files, raw_tables
from .dbt_assets import build_dbt_assets
from .resources import PipelineResource
from .sensors import email_on_check_warnings, email_on_run_failure, email_on_stale_data

# 06:17 Toronto time: after OpenNeuro's overnight activity, and off the hour
# so it does not compete with the many jobs everyone schedules at :00.
DAILY_CRON = "17 6 * * *"
TIMEZONE = "America/Toronto"


def build_definitions(settings: Settings | None = None) -> Definitions:
    """Assemble the Dagster definitions (a function so tests can inject settings)."""
    settings = settings or Settings.from_env()
    dbt_models, dbt_resource = build_dbt_assets(settings)

    daily_refresh = define_asset_job(
        name="daily_refresh",
        selection=AssetSelection.all(),
        description="Extract the catalogue and changed participant files, load, dbt build.",
    )
    daily_schedule = ScheduleDefinition(
        name="daily_refresh_schedule",
        job=daily_refresh,
        cron_schedule=DAILY_CRON,
        execution_timezone=TIMEZONE,
        # Off until switched on in the UI, so a fresh clone never starts
        # calling the OpenNeuro API on its own.
        default_status=DefaultScheduleStatus.STOPPED,
    )

    return Definitions(
        assets=[openneuro_catalogue, openneuro_participant_files, raw_tables, dbt_models],
        jobs=[daily_refresh],
        schedules=[daily_schedule],
        sensors=[email_on_run_failure, email_on_check_warnings, email_on_stale_data],
        resources={"pipeline": PipelineResource(), "dbt": dbt_resource},
    )

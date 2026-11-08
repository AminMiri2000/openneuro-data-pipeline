"""Dagster resources: how assets get a configured `Pipeline`."""

from dagster import ConfigurableResource

from ..config import Settings
from ..pipeline import Pipeline


class PipelineResource(ConfigurableResource["PipelineResource"]):
    """Builds the pipeline from the same environment variables as the CLI.

    Settings are read when a run starts, not when Dagster loads the code, so
    changing an environment variable takes effect on the next run.
    """

    def get_pipeline(self) -> Pipeline:
        return Pipeline(Settings.from_env())

"""Run the dbt project in-process.

dbt is called through its official Python API (`dbtRunner`) rather than a
subprocess, so the pipeline needs no `dbt` executable on PATH (which differs
between Windows and Unix virtualenvs) and gets structured results back.

Generated dbt files (compiled SQL, run artefacts, logs) go under the data
directory next to the warehouse, keeping the repository clean and giving each
data directory, including test fixtures, its own isolated dbt state.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..config import Settings
from ..errors import ConfigError, TransformError

log = logging.getLogger(__name__)

# Statuses that mean dbt did not produce trustworthy output.
FAILED_STATUSES = frozenset({"error", "fail", "runtime error"})


@dataclass
class TransformResult:
    """Outcome of a dbt invocation.

    Attributes:
        command: The dbt command run, e.g. "build".
        status_counts: Nodes per final status ("success", "pass", "warn", ...).
        warnings: Names of nodes that ended in "warn", e.g. warn-only tests.
    """

    command: str
    status_counts: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@contextmanager
def _temporary_env(values: Mapping[str, str]) -> Iterator[None]:
    """Set environment variables for the duration of a block, then restore them."""
    previous = {key: os.environ.get(key) for key in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for key, old in previous.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


def _release_duckdb() -> None:
    """Close the DuckDB connection dbt-duckdb keeps open after a run.

    dbt-duckdb caches one connection per process and does not close it when
    `dbtRunner.invoke` returns. While it is open, DuckDB refuses a connection
    with different settings (e.g. read-only) to the same file, so later steps
    in this process would fail. Its own `close_all_connections` only drops the
    reference, so the cached environment is closed explicitly.
    """
    try:
        from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    except ImportError:  # pragma: no cover - dbt-duckdb is a declared dependency
        return
    environment = getattr(DuckDBConnectionManager, "_ENV", None)
    if environment is not None:
        environment.close()
    DuckDBConnectionManager.close_all_connections()  # type: ignore[no-untyped-call]


class DbtProject:
    """The dbt project under `Settings.dbt_project_dir`."""

    def __init__(self, settings: Settings) -> None:
        self.project_dir = settings.dbt_project_dir.resolve()
        self.target_dir = (settings.data_dir / "dbt" / "target").resolve()
        self._env = {
            # Read by profiles.yml, so dbt targets the same warehouse file.
            "PIPELINE_WAREHOUSE_PATH": str(settings.warehouse_path.resolve()),
            # Honoured by every dbt command (unlike the equivalent CLI flags).
            "DBT_TARGET_PATH": str(self.target_dir),
            "DBT_LOG_PATH": str((settings.data_dir / "dbt" / "logs").resolve()),
        }

    def build(self, select: str | None = None, full_refresh: bool = False) -> TransformResult:
        """Run seeds, models and tests in dependency order (`dbt build`)."""
        args = ["build"]
        if select:
            args += ["--select", select]
        if full_refresh:
            args.append("--full-refresh")
        return self.invoke(args)

    def generate_docs(self) -> Path:
        """Build the documentation site (manifest + catalog) and return its folder."""
        self.invoke(["docs", "generate"])
        return self.target_dir

    def invoke(self, args: Sequence[str]) -> TransformResult:
        """Run any dbt command, e.g. `["docs", "generate"]`.

        Raises:
            ConfigError: The dbt project folder does not exist.
            TransformError: dbt failed, or a blocking (error-severity) test failed.
        """
        if not (self.project_dir / "dbt_project.yml").is_file():
            raise ConfigError(
                f"No dbt project at {self.project_dir}. Run from the repository root "
                "or set PIPELINE_DBT_PROJECT_DIR."
            )
        # Imported lazily: dbt takes a second or two to import, which commands
        # that never transform (extract, load, query) should not pay for.
        from dbt.cli.main import dbtRunner

        full_args = [
            *args,
            "--project-dir",
            str(self.project_dir),
            "--profiles-dir",
            str(self.project_dir),
        ]
        log.info("transform: dbt %s", " ".join(args))
        with _temporary_env(self._env):
            try:
                outcome = dbtRunner().invoke(full_args)
            finally:
                _release_duckdb()

        result = self._summarise(" ".join(args), outcome.result)
        if not outcome.success:
            detail = outcome.exception or f"node statuses: {result.status_counts}"
            raise TransformError(f"dbt {args[0]} failed ({detail}); see the dbt log above")
        return result

    @staticmethod
    def _summarise(command: str, run_result: Any) -> TransformResult:
        """Count node statuses for commands that run nodes (build, run, test, seed)."""
        result = TransformResult(command=command)
        node_results = getattr(run_result, "results", None)
        if not node_results:
            return result
        statuses: Counter[str] = Counter()
        for node_result in node_results:
            status = str(node_result.status).lower()
            statuses[status] += 1
            if status == "warn":
                result.warnings.append(node_result.node.name)
        result.status_counts = dict(statuses)
        return result

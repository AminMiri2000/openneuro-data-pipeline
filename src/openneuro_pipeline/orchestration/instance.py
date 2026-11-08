"""The local Dagster instance: where run history, schedules and logs live.

Dagster reads its settings from `$DAGSTER_HOME/dagster.yaml`. The file is
written from `INSTANCE_CONFIG` on every start, so the repository stays the
single source of truth and there is nothing to set up by hand.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ..config import Settings

INSTANCE_CONFIG: dict[str, Any] = {
    # DuckDB allows one writer at a time, so runs are queued and executed one
    # by one. A scheduled run that starts while a manual one is going waits
    # instead of failing on a locked warehouse.
    "run_queue": {"max_concurrent_runs": 1},
    # Re-run a failed run once, from the failed step only. Combined with the
    # per-asset retry policy this covers both short and longer outages.
    "run_retries": {"enabled": True, "max_retries": 1},
    "telemetry": {"enabled": False},
}


def dagster_home(settings: Settings) -> Path:
    """Folder for Dagster's state, next to the warehouse and outside git."""
    return (settings.data_dir / "dagster").resolve()


def prepare_instance(settings: Settings) -> Path:
    """Create DAGSTER_HOME with an up-to-date dagster.yaml and return its path."""
    home = dagster_home(settings)
    home.mkdir(parents=True, exist_ok=True)
    # JSON is valid YAML, and needs no extra dependency to write.
    (home / "dagster.yaml").write_text(json.dumps(INSTANCE_CONFIG, indent=2), encoding="utf-8")
    return home

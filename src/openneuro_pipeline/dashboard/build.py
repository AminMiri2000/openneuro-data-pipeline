"""Build the static dashboard: query the marts, embed the results in one HTML page.

The output is a single self-contained `index.html` (plus a `data.json` copy
for anyone who wants the numbers), so it can be hosted anywhere static files
are served: GitHub Pages, a file share, or opened straight from disk. Nothing
queries the warehouse at view time.

All numbers come from the dbt marts, never from raw tables, so the dashboard
shows exactly what the tested models say.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb

from ..config import Settings
from ..errors import StorageError

log = logging.getLogger(__name__)

TEMPLATE = "template.html"
DATA_PLACEHOLDER = "/*__DASHBOARD_DATA__*/null"

# Each query returns rows as dicts. Kept as plain SQL against the marts so
# they can be copied into `make query` to check any number on the page.
QUERIES: dict[str, str] = {
    "summary": """
        select
            count(*) filter (where is_active)                           as n_datasets,
            sum(n_participant_rows) filter (where is_active)            as n_participants,
            sum(n_subjects) filter (where is_active)                    as n_subjects,
            round(sum(size_bytes) filter (where is_active) / 1e12, 2)  as total_tb,
            count(*) filter (where is_active and has_participants_table) as n_with_participants,
            max(last_seen_date)                                         as latest_ingest_date
        from marts.dim_dataset
    """,
    "demographics_known": """
        select
            round(count(age_years) / count(*), 4)   as age,
            round(count(sex) / count(*), 4)         as sex,
            round(count(handedness) / count(*), 4)  as handedness
        from marts.fct_participant
    """,
    "datasets_by_year": """
        select cast(year(dataset_created_at) as varchar) as label, count(*) as value
        from marts.dim_dataset
        where is_active and dataset_created_at is not null
        group by all order by label
    """,
    "subjects_per_dataset": """
        select label, count(*) as value
        from (
            select case
                when n_subjects < 10  then '1-9'
                when n_subjects < 20  then '10-19'
                when n_subjects < 50  then '20-49'
                when n_subjects < 100 then '50-99'
                when n_subjects < 250 then '100-249'
                else '250+'
            end as label,
            case
                when n_subjects < 10  then 1 when n_subjects < 20  then 2
                when n_subjects < 50  then 3 when n_subjects < 100 then 4
                when n_subjects < 250 then 5 else 6
            end as bucket
            from marts.dim_dataset
            where is_active and n_subjects > 0
        )
        group by label, bucket order by bucket
    """,
    "age_distribution": """
        select label, count(*) as value
        from (
            select case
                when age_years < 10 then '0-9'
                when age_years < 18 then '10-17'
                when age_years < 30 then '18-29'
                when age_years < 45 then '30-44'
                when age_years < 65 then '45-64'
                else '65+'
            end as label,
            case
                when age_years < 10 then 1 when age_years < 18 then 2
                when age_years < 30 then 3 when age_years < 45 then 4
                when age_years < 65 then 5 else 6
            end as bucket
            from marts.fct_participant
            where age_years is not null
        )
        group by label, bucket order by bucket
    """,
    "top_tasks": """
        select lower(task) as label, count(distinct dataset_id) as value
        from (select dataset_id, unnest(tasks) as task from marts.dim_dataset where is_active)
        where task is not null and trim(task) <> ''
        group by all order by value desc, label limit 12
    """,
    "standardisation": """
        select
            attribute,
            count(*) filter (where parse_status = 'ok')                     as standardised,
            count(*) filter (where parse_status = 'missing')                as empty_in_source,
            count(*) filter (where parse_status not in ('ok', 'missing'))   as not_standardisable
        from intermediate.int_participant_attribute_values
        group by attribute
        order by case attribute when 'age' then 1 when 'sex' then 2 else 3 end
    """,
    "catalogue_history": """
        select cast(ingest_date as varchar) as label, count(*) as value
        from marts.fct_dataset_daily
        group by ingest_date order by ingest_date
    """,
    "datasets": """
        select
            dataset_id,
            dataset_name,
            openneuro_url,
            n_subjects,
            size_gb,
            year(dataset_created_at)         as year,
            tasks,
            pct_age_known,
            pct_sex_known,
            age_median_years
        from marts.dim_dataset
        where is_active
        order by n_subjects desc nulls last, dataset_id
    """,
}


@dataclass(frozen=True)
class DashboardBuild:
    """Where the dashboard was written and how many datasets it covers."""

    output_dir: Path
    n_datasets: int


def _json_default(value: Any) -> Any:
    """Make DuckDB result values JSON-serialisable."""
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, int):  # HUGEINT and friends arrive as plain ints already
        return value
    return str(value)


def _rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = con.execute(sql)
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def collect(settings: Settings) -> dict[str, Any]:
    """Run every dashboard query and return the page's data."""
    if not settings.warehouse_path.exists():
        raise StorageError("No warehouse yet; run `make run` before building the dashboard.")
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        data: dict[str, Any] = {name: _rows(con, sql) for name, sql in QUERIES.items()}
    # Single-row results are more convenient as objects.
    data["summary"] = data["summary"][0]
    data["demographics_known"] = data["demographics_known"][0]
    data["quality"] = _quality_summary(settings)
    data["generated_at"] = datetime.now(UTC).isoformat(timespec="seconds")
    data["repo_url"] = _repo_url()
    data["docs_url"] = None
    return data


def _repo_url() -> str | None:
    """Link to the source code: DASHBOARD_REPO_URL, or the repo GitHub Actions runs in."""
    if url := os.environ.get("DASHBOARD_REPO_URL"):
        return url
    server, repo = os.environ.get("GITHUB_SERVER_URL"), os.environ.get("GITHUB_REPOSITORY")
    return f"{server}/{repo}" if server and repo else None


def _quality_summary(settings: Settings) -> dict[str, Any] | None:
    """Data-test results from the last `dbt build`, if one ran in this data directory."""
    path = settings.data_dir / "dbt" / "target" / "run_results.json"
    if not path.is_file():
        return None
    results = json.loads(path.read_text(encoding="utf-8")).get("results", [])
    tests = [r for r in results if r.get("unique_id", "").startswith(("test.", "unit_test."))]
    by_status: dict[str, int] = {}
    for test in tests:
        by_status[test["status"]] = by_status.get(test["status"], 0) + 1
    return {
        "n_tests": len(tests),
        "by_status": by_status,
        "warnings": sorted(
            t["unique_id"].split(".")[2] for t in tests if t["status"] in {"warn", "fail", "error"}
        ),
    }


def render(data: dict[str, Any]) -> str:
    """Insert the data into the HTML template."""
    template = resources.files(__package__).joinpath(TEMPLATE).read_text(encoding="utf-8")
    # "</" inside a <script> block would end it early; JSON allows the escape.
    payload = json.dumps(data, default=_json_default, ensure_ascii=False).replace("</", "<\\/")
    if DATA_PLACEHOLDER not in template:
        raise StorageError("Dashboard template is missing its data placeholder")
    return template.replace(DATA_PLACEHOLDER, payload)


DOCS_FILES = ("index.html", "manifest.json", "catalog.json")
DOCS_SUBDIR = "dbt-docs"


def build(
    settings: Settings, output_dir: Path | None = None, docs_dir: Path | None = None
) -> DashboardBuild:
    """Write index.html and data.json to `output_dir` (default: data/site).

    With `docs_dir` (a generated dbt docs site), the docs are copied to
    `<output_dir>/dbt-docs/` and linked from the page, so one static site
    serves both the dashboard and the data model documentation.
    """
    output_dir = (output_dir or settings.data_dir / "site").resolve()
    data = collect(settings)
    output_dir.mkdir(parents=True, exist_ok=True)
    if docs_dir is not None:
        target = output_dir / DOCS_SUBDIR
        target.mkdir(exist_ok=True)
        for name in DOCS_FILES:
            shutil.copy2(docs_dir / name, target / name)
        data["docs_url"] = f"{DOCS_SUBDIR}/"
    (output_dir / "index.html").write_text(render(data), encoding="utf-8")
    (output_dir / "data.json").write_text(
        json.dumps(data, default=_json_default, indent=2), encoding="utf-8"
    )
    log.info("dashboard: wrote %s (%d datasets)", output_dir, len(data["datasets"]))
    return DashboardBuild(output_dir=output_dir, n_datasets=len(data["datasets"]))

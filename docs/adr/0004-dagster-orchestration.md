# ADR 0004: Orchestration with Dagster

- Status: accepted
- Date: 2026-11-08

## Context

The pipeline ran only when someone typed `make run`. A real pipeline needs a
schedule, retries for a flaky public API, a guarantee that two runs never
write to DuckDB at once, and a way to see what ran, what failed and what each
table depends on.

## Decision

1. **Dagster, asset-based.** Each step is an asset: the catalogue file,
   participant files, three raw tables, and every dbt model and seed (via
   `dagster-dbt`). The load step's asset keys equal the dbt source keys, so
   the UI shows one connected graph from the API to each mart. dbt tests
   become Dagster asset checks.
2. **Assets call the existing `Pipeline` facade.** Orchestration adds
   scheduling and observability; it does not reimplement any step. The CLI
   and Dagster run the same code.
3. **One run at a time.** `dagster.yaml` sets `max_concurrent_runs: 1`, so a
   scheduled run queues behind a manual one instead of failing on a locked
   warehouse.
4. **Retries where failures are transient.** API-facing assets retry three
   times with exponential backoff (1, 2, 4 minutes); a failed run is retried
   once from the failed step. Participant downloads that fail for individual
   datasets raise a *warning* asset check instead of failing the run, since
   they are retried automatically next time and everything else should still
   load.
5. **Daily schedule at 06:17 Toronto time, off by default.** Off the hour to
   avoid the :00 rush; off by default so a fresh clone never calls the API on
   its own.
6. **Instance config generated from code.** `openneuro-pipeline dagster`
   writes `data/dagster/dagster.yaml` from `orchestration/instance.py` on every
   start, so there is nothing to configure by hand.

## Backfills, honestly

OpenNeuro's API returns only the current catalogue. Re-extracting "last
Tuesday" would store today's data labelled as Tuesday, so the earlier
`make backfill DATE=...` target was misleading and has been removed. What
*can* be replayed is everything after extraction: `make rebuild` reloads the
raw tables and rebuilds every dbt model (including the incremental history)
from the raw files already saved. Daily raw partitions are therefore the
system of record for history, and are never overwritten except by a re-run
of the same day.

## Alternatives considered

- **Airflow.** More widespread, but needs a scheduler, webserver and
  metadata database to run locally, and is task-based: lineage between tables
  would have to be described by hand. Dagster derives it from the code and
  the dbt manifest.
- **Cron / Windows Task Scheduler + `make run`.** Free and simple, but no
  retries, no run history, no concurrency control and nothing to show.
- **Daily partitions in Dagster.** Would make "backfill this date" a UI
  button, but for this source it would be a button that stores wrong data.
  Partitions fit sources that can serve a past period.

## Consequences

- `make dagster` starts the UI, daemon and schedules on localhost:3000.
- **Python 3.11–3.13.** `dagster-dbt` does not yet support 3.14, so the upper
  bound moves down from ADR 0003's 3.14.
- **Dependency pins.** dbt needs protobuf 6, while the latest
  `grpcio-health-checking` (pulled in by Dagster) needs protobuf 7, so both are
  pinned until dbt supports protobuf 7.
- The test suite runs the real `daily_refresh` job end to end against mocked
  HTTP, including a dbt build through dagster-dbt.

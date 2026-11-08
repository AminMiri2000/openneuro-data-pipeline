# OpenNeuro Data Pipeline

[![CI](https://github.com/AminMiri2000/openneuro-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/AminMiri2000/openneuro-data-pipeline/actions/workflows/ci.yml)

A batch data pipeline that turns [OpenNeuro](https://openneuro.org), the largest
open neuroimaging archive, into a queryable, tested warehouse. The goal is to
answer questions the OpenNeuro website can't, such as:

> *Which EEG datasets have at least 20 subjects, known age and sex for nearly
> every participant, and a visual task?*

It costs nothing to run: public APIs, local storage, DuckDB, dbt and other
open-source tools.

## Architecture

```mermaid
flowchart LR
    subgraph Source
        GQL[OpenNeuro GraphQL API]
        S3[(OpenNeuro public S3)]
    end
    subgraph Raw["Raw layer (data/raw)"]
        DS[datasets/<br/>ingest_date=…/modality=…<br/>JSONL.gz]
        PT[participants/<br/>dataset_id=…/snapshot_tag=…<br/>TSV + JSON sidecar]
    end
    subgraph WH["DuckDB warehouse"]
        RAW[raw.*]
        STG[staging.*]
        INT[intermediate.*]
        MART[marts.*]
    end
    GQL -- daily full snapshot --> DS
    S3 -- incremental by snapshot --> PT
    DS -- incremental load --> RAW
    PT -- incremental load --> RAW
    RAW -- dbt --> STG -- dbt --> INT -- dbt --> MART
    MART --> DASH[Dashboard]
    classDef todo stroke-dasharray: 5 5
    class DASH todo
```

Dashed boxes are later phases.

## Data model

| Table | Grain | Use it for |
|---|---|---|
| `marts.dim_dataset` | one row per dataset | finding datasets: metadata, tasks, size, and how complete each one's demographics are |
| `marts.fct_participant` | one row per participant | standardised age (years), sex and handedness across all datasets |
| `marts.fct_dataset_daily` | one row per dataset per day | how datasets grow over time (incremental model) |

Participant tables are messy: the same field appears as `age`/`Age`,
`sex`/`gender`/`Gender` or `hand`/`handedness`, with values such as `F`, `f`,
`Female` or a numeric code. The dbt models standardise them in five steps:

1. **Unpivot** each participant's columns into rows.
2. **Match** column names to standard attributes through a lookup seed.
3. **Decode** numeric codes with the dataset's own data dictionary
   (`participants.json` `Levels`, e.g. `"2"` → `"Female"`).
4. **Convert** to standard values, honouring declared units (age in months → years),
   by exact lookup first, then by whole-word patterns for descriptive labels
   such as "Physiological female" or "Right handed (LQ > 0)".
5. **Classify** anything unusable (`unmapped`, `ambiguous`, `undocumented_numeric`,
   `unparseable`, `out_of_range`) instead of silently dropping it, so data
   quality can be measured.

Extending coverage for a new spelling is a one-line CSV change in
[`transform/seeds/`](transform/seeds), not a SQL change. Every model and column
is documented; run `make docs-serve` to browse them with the lineage graph.

## Design decisions

| Decision | Why |
|---|---|
| **Catalogue: daily full snapshot**, partitioned by ingest date | A few thousand rows; re-pulling is cheaper than change-tracking, and the daily partitions give free history of how each dataset evolved. |
| **Participants: incremental by snapshot tag** | One HTTP call per dataset is the expensive part, so it only runs when a dataset publishes a new snapshot. |
| **Incremental load** | A load log stores each raw partition's fingerprint; only new or changed partitions are loaded, each in its own transaction. Load time grows with new data, not history. |
| **Incremental dbt history** | `fct_dataset_daily` re-processes only a short look-back window each run, with delete+insert on its key so re-runs never duplicate. |
| **Raw stays raw; dbt does the cleaning** | Payloads are stored exactly as returned. An API change breaks a visible, tested model, not the loader. |
| **Tests at every layer** | Python unit and integration tests; dbt data tests on every model; dbt unit tests for the standardisation edge cases found in real data. |
| **Parallel, bounded downloads** | Per-dataset fetches run on a small thread pool with thread-local HTTP sessions. |
| **Idempotent, atomic writes** | Deterministic output paths, temp file + rename. Re-running a day overwrites it; a crash leaves nothing half-written. |
| **Failures are isolated and retried** | A dataset that fails is logged, its watermark isn't advanced, and the next run retries it. |
| **Swappable storage** | The pipeline talks to an `ObjectStore` interface; local disk today, S3/GCS by adding one class. |

More detail: [architecture and scalability](docs/architecture.md) and the
decision records:
[0001 free, local-first stack](docs/adr/0001-free-local-first-stack.md),
[0002 layered, incremental design](docs/adr/0002-layered-incremental-design.md),
[0003 dbt modelling and standardisation](docs/adr/0003-dbt-modelling.md).

## Quick start

Requires **Python 3.11 to 3.14** and GNU make. Works on Windows (PowerShell or
cmd), macOS and Linux. On Windows, install make with
`winget install GnuWin32.Make` or `choco install make`.

```bash
make install       # virtualenv, dependencies, git hooks
make check-api     # confirm OpenNeuro answers (prints one page)
make run           # extract + load + dbt build (a few minutes for all EEG datasets)
make docs-serve    # browse model docs and lineage at http://localhost:8080
make help          # every other target
```

If your default Python is outside that range, point make at another one, e.g.
`make install SYSTEM_PYTHON="py -3.13"` on Windows.

Then query the marts (read-only):

```bash
make query SQL="SELECT dataset_id, dataset_name, n_subjects, pct_age_known, pct_sex_known FROM marts.dim_dataset ORDER BY n_subjects DESC LIMIT 10"
```

The headline question is answered by
[`transform/analyses/find_eeg_datasets.sql`](transform/analyses/find_eeg_datasets.sql).

Useful options: `--modality MEG|MRI|iEEG|all`, `--limit 20` for a quick trial,
`run --skip-transform`, `transform --select marts`, and
`make backfill DATE=2026-11-01`. Any dbt command works through the pipeline,
e.g. `openneuro-pipeline dbt source freshness`. Environment variables are
listed in [`config.py`](src/openneuro_pipeline/config.py).

## Project layout

```
src/openneuro_pipeline/
  sources/openneuro/   GraphQL + S3 client: retries, pagination, thread-local sessions
  storage/             ObjectStore interface + local implementation, key layout,
                       watermarks, file formats
  extract/             Extractor base class, registry, one extractor per entity
  load/                BIDS parsers, one loader per raw table, incremental warehouse load
  transform/           runs dbt in-process via its Python API
  pipeline.py          wires the layers together (used by the CLI, later by Dagster)
  cli.py               thin command-line entry point
transform/             dbt project
  models/staging/      1:1 with raw tables: typed and renamed (views)
  models/intermediate/ attribute standardisation (views)
  models/marts/        dim_dataset, fct_participant, fct_dataset_daily (tables)
  seeds/               lookup tables for column names and values
  analyses/            example questions answered with the marts
tests/                 Python unit and integration tests (HTTP mocked)
scripts/dev.py         cross-platform helpers used by the Makefile
docs/                  architecture, scalability notes, decision records
```

## Testing

```bash
make test        # 60 Python tests (including a full dbt build), ~97% coverage
make transform   # dbt build: 42 data tests + 2 unit tests on your own data
make lint        # ruff
make typecheck   # mypy --strict
```

CI runs lint, strict type checking and tests on Python 3.11–3.14 and on Windows
for every push and pull request, and fails below 90% coverage. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the branch workflow and conventions.

## Roadmap

- [x] **Phase 1:** batch ingestion to raw storage, DuckDB raw tables, tests, CI
- [x] **Phase 2:** dbt staging, intermediate and mart models with tests and docs
- [ ] **Phase 3:** Dagster orchestration: schedules, retries, backfills
- [ ] **Phase 4:** data quality alerts, dashboard
- [ ] **Phase 5:** write-up and walkthrough video

## Data and licence

Dataset metadata comes from OpenNeuro; each dataset carries its own licence
(mostly CC0). This repository stores no data, only code. Code is MIT licensed.

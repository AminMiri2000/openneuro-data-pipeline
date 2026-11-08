# Architecture

## Layers

```
cli ─┐
     ├─> pipeline ──> extract ──> sources (OpenNeuro client)
dagster (phase 3) ┘        │  └──> storage (object store, layout, state)
                           ├─> load ───> storage
                           │        └──> DuckDB warehouse (raw schema)
                           └─> transform ──> dbt project (transform/)
                                         └──> DuckDB warehouse (staging, intermediate, marts)
```

Each layer depends only on the layers below it.

| Package | Responsibility | Knows nothing about |
|---|---|---|
| `sources/` | Talking to upstream APIs: retries, pagination, errors | where data is stored |
| `storage/` | Raw-layer objects, key layout, watermarks, file formats | APIs, databases |
| `extract/` | One extractor per entity: source → raw layer | the warehouse |
| `load/` | Raw layer → DuckDB, one loader per table | APIs |
| `transform/` | Running the dbt project in-process | how models are written |
| `transform/` (dbt project, repo root) | SQL models, seeds and data tests: raw → marts | Python |
| `pipeline.py` | Wiring the above together; the only object entry points use | CLI options |
| `cli.py` | Parsing options, printing results, exit codes | how anything works |

Because of this split, the CLI and the phase-3 Dagster assets share one code
path, and any implementation can be swapped by changing `pipeline.py` alone.

## Scalability

Each concern below names the limit it addresses and how to grow past it.

| Concern | Current design | How it grows |
|---|---|---|
| Catalogue size | Generator-based pagination; records streamed straight into a gzip file | Memory stays flat at one page, whatever the catalogue size |
| Per-dataset downloads | Bounded thread pool (`PIPELINE_MAX_WORKERS`), thread-local HTTP sessions | Raise workers; or shard by dataset id across processes/machines, since targets are independent |
| Re-processing | Incremental extract (snapshot watermark) and incremental load (partition fingerprints) | Work per run is proportional to *new* data, not total history |
| Warehouse inserts | Catalogue loaded by DuckDB's native JSON reader; Python-parsed rows inserted in batches (`PIPELINE_INSERT_BATCH_SIZE`) | Bounded memory per batch |
| Crash recovery | Atomic object writes, periodic watermark checkpoints, one transaction per partition | A re-run resumes from the first unfinished unit of work |
| Storage location | `ObjectStore` protocol; local disk today | Add an S3/GCS class; DuckDB reads `s3://` keys via `httpfs`. Layout is Hive-partitioned so Spark/Athena/BigQuery can read it too |
| Warehouse engine | DuckDB, single writer | Swap `Warehouse` for a server warehouse; the dbt models move with an adapter change (`profiles.yml`) |
| Transform volume | Views for staging/intermediate, tables for marts, incremental `fct_dataset_daily` | Make further marts incremental on their unique keys when they outgrow full rebuilds (see ADR 0003) |
| Attribute coverage | Column and value mappings in dbt seeds | Add CSV rows; no code change |
| New data | Extractor and table-loader registries | See "Adding an entity" below |

### Known limits

- DuckDB permits one writer, so two pipeline runs must not load at the same
  time. The orchestrator (phase 3) will enforce a single concurrent run.
- The watermark is a JSON file. Beyond ~100k keys it should move into a
  database table, a change contained in `storage/state.py`.

## Adding an entity

Example: ingesting each dataset's `events.tsv` files.

1. **Extract:** create `extract/events.py` with a class that subclasses
   `Extractor`, sets `name = "events"` and `depends_on = ("datasets",)`, and is
   decorated with `@register`. Import it in `extract/__init__.py`.
2. **Layout:** add its key functions to `storage/layout.py`.
3. **Load:** add a `RawTableLoader` subclass in `load/tables.py` and append it to
   `TABLE_LOADERS`.
4. **Model:** declare the raw table in `_openneuro__sources.yml`, add a
   `stg_openneuro__events` model with documented, tested columns, and build
   marts on top of it.
5. **Test:** add unit tests for any parser, an integration test using the
   `mocked_api` fixture, and dbt unit tests for non-trivial SQL logic.

The CLI (`openneuro-pipeline extract events`), run manifests, incremental
loading and the load log all work without further changes.

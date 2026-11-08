# ADR 0001: A free, local-first stack

- Status: accepted
- Date: 2026-11-07

## Context

This is a portfolio project. It must cost $0 to build and run, must be
reproducible by a reviewer with one command, and should still use the tools
data engineering teams use in production.

## Decision

| Concern        | Choice                               | Cost |
|----------------|--------------------------------------|------|
| Source         | OpenNeuro GraphQL API + public S3    | free, no account |
| Raw storage    | Local disk, Hive-style partitions    | free |
| Warehouse      | DuckDB (single file)                 | free |
| Transform      | dbt-core + dbt-duckdb (phase 2)      | free |
| Orchestration  | Dagster OSS, run locally (phase 3)   | free |
| CI             | GitHub Actions on a public repo      | free |
| Dashboard      | Evidence or Streamlit (phase 4)      | free |

## Alternatives considered

- **BigQuery / Snowflake.** Closer to many job ads, but free tiers expire or need
  a credit card, and a reviewer cannot run the project without an account.
  dbt models are portable, so swapping the adapter later is a small change;
  that portability is itself worth demonstrating.
- **Airflow.** More widespread, but heavy to run locally (scheduler, webserver,
  metadata DB). Dagster's asset model also maps more naturally onto dbt models.
- **MinIO for object storage.** Adds a container without adding a skill the
  partition layout doesn't already show. Kept as an optional later step.

## Consequences

- Everything runs on a laptop; `make install && make run` reproduces the data.
- The raw layout (`source/entity/key=value/...`) is the same one you'd use on
  S3 or GCS, so moving to cloud storage is a path change, not a redesign.
- DuckDB is single-writer: fine for a batch pipeline, not for concurrent jobs.

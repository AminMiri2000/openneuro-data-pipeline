"""Dagster orchestration: assets, schedule and the local Dagster instance.

Every step of the pipeline is a Dagster asset, so the UI shows one lineage
graph from the OpenNeuro API through raw files and raw tables to each dbt
model. Assets call the same `Pipeline` facade as the CLI: orchestration adds
scheduling, retries and observability, not a second implementation.

Entry point for Dagster: `openneuro_pipeline.orchestration.definitions`.
"""

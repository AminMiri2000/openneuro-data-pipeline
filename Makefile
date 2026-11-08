.PHONY: help install check-api extract load run backfill test lint typecheck format clean

PIPELINE := .venv/bin/openneuro-pipeline
MODALITY ?= EEG

help:               ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

install:            ## create a virtualenv, install the project and git hooks
	python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
	.venv/bin/pre-commit install

check-api:          ## confirm the OpenNeuro API answers our query
	$(PIPELINE) check-api

extract:            ## all extractors into data/raw (MODALITY=EEG by default)
	$(PIPELINE) extract --modality $(MODALITY)

load:               ## load new or changed raw partitions into DuckDB
	$(PIPELINE) load

run:                ## full batch: extract + load
	$(PIPELINE) run --modality $(MODALITY)

backfill:           ## re-extract a given day: make backfill DATE=2026-11-01
	$(PIPELINE) run --modality $(MODALITY) --ingest-date $(DATE)

test:               ## tests with coverage gate
	.venv/bin/pytest --cov=openneuro_pipeline --cov-report=term-missing

lint:               ## lint and format check
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

typecheck:          ## mypy strict
	.venv/bin/mypy

format:             ## auto-format and auto-fix lint
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

clean:              ## delete local data and warehouse (raw files are re-downloadable)
	rm -rf data/raw data/warehouse.duckdb

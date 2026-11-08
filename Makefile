.PHONY: install check-api ingest load run test lint format clean

install:            ## create a virtualenv and install the project with dev tools
	python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"

check-api:          ## confirm the OpenNeuro API answers our query
	.venv/bin/openneuro-pipeline check-api

ingest:             ## pull EEG dataset catalogue + participant tables into data/raw
	.venv/bin/openneuro-pipeline ingest datasets --modality EEG
	.venv/bin/openneuro-pipeline ingest participants --modality EEG

load:               ## rebuild DuckDB raw tables from data/raw
	.venv/bin/openneuro-pipeline load

run:                ## full batch: ingest + load
	.venv/bin/openneuro-pipeline run --modality EEG

test:
	.venv/bin/pytest --cov=openneuro_pipeline --cov-report=term-missing

lint:
	.venv/bin/ruff check . && .venv/bin/ruff format --check .

format:
	.venv/bin/ruff format . && .venv/bin/ruff check --fix .

clean:              ## delete local data and warehouse (raw files are re-downloadable)
	rm -rf data/raw data/state data/warehouse.duckdb

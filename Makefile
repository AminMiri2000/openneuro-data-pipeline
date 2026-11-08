# Cross-platform Makefile: works with GNU make on Windows (cmd/PowerShell),
# macOS and Linux.
#
# Portability rules used below:
#   * Every tool runs as `<venv python> -m <module>`, never as a console script,
#     because the venv's executables live in .venv\Scripts on Windows but
#     .venv/bin elsewhere.
#   * No Unix-only commands (rm, grep, awk): file and text work is done in
#     Python, via scripts/dev.py.

ifeq ($(OS),Windows_NT)
    SYSTEM_PYTHON ?= python
    VENV_PYTHON := .venv\Scripts\python.exe
else
    SYSTEM_PYTHON ?= python3
    VENV_PYTHON := .venv/bin/python
endif

PIPELINE := $(VENV_PYTHON) -m openneuro_pipeline.cli
MODALITY ?= EEG

.PHONY: help install check-api extract load run backfill query test lint typecheck format clean

help:               ## list targets
	@$(SYSTEM_PYTHON) scripts/dev.py help Makefile

install:            ## create a virtualenv, install the project and git hooks
	$(SYSTEM_PYTHON) -m venv .venv
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -e ".[dev]"
	$(VENV_PYTHON) -m pre_commit install

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

query:              ## read-only SQL on the warehouse: make query SQL="SELECT ..."
	$(PIPELINE) query "$(SQL)"

test:               ## tests with coverage gate
	$(VENV_PYTHON) -m pytest --cov=openneuro_pipeline --cov-report=term-missing

lint:               ## lint and format check
	$(VENV_PYTHON) -m ruff check .
	$(VENV_PYTHON) -m ruff format --check .

typecheck:          ## mypy strict
	$(VENV_PYTHON) -m mypy

format:             ## auto-format and auto-fix lint
	$(VENV_PYTHON) -m ruff format .
	$(VENV_PYTHON) -m ruff check --fix .

clean:              ## delete local data and warehouse (raw files are re-downloadable)
	$(SYSTEM_PYTHON) scripts/dev.py clean

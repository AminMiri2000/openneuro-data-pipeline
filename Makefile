# Cross-platform Makefile: works with GNU make on Windows (cmd/PowerShell),
# macOS and Linux.
#
# Portability rules used below:
#   * Every tool runs as `<venv python> -m <module>`, never as a console script,
#     because the venv's executables live in .venv/Scripts on Windows but
#     .venv/bin elsewhere.
#   * On Windows, make may run recipes through cmd.exe (which rejects unquoted
#     forward-slash paths) or through Git's bash (which strips backslashes).
#     A quoted forward-slash path works in both.
#   * No Unix-only commands (rm, grep, awk): file and text work is done in
#     Python, via scripts/dev.py.

ifeq ($(OS),Windows_NT)
    SYSTEM_PYTHON ?= python
    VENV_PYTHON := ".venv/Scripts/python.exe"
else
    SYSTEM_PYTHON ?= python3
    VENV_PYTHON := .venv/bin/python
endif

PIPELINE := $(VENV_PYTHON) -m openneuro_pipeline.cli
MODALITY ?= EEG

.PHONY: help install check-api extract load transform run rebuild dagster query dashboard docs \
        docs-serve freshness test lint typecheck format clean clean-venv

help:               ## list targets
	@$(SYSTEM_PYTHON) scripts/dev.py help Makefile

install:            ## create a virtualenv, install the project and git hooks
	$(SYSTEM_PYTHON) scripts/dev.py check-python
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

transform:          ## build dbt models and run data tests
	$(PIPELINE) transform

run:                ## full batch: extract + load + transform
	$(PIPELINE) run --modality $(MODALITY)

rebuild:            ## rebuild warehouse + dbt from saved raw files (no API calls)
	$(PIPELINE) load --full-refresh
	$(PIPELINE) transform --full-refresh

dagster:            ## Dagster UI, daily schedule and run queue at http://localhost:3000
	$(PIPELINE) dagster --port 3000

query:              ## read-only SQL on the warehouse: make query SQL="SELECT ..."
	$(PIPELINE) query "$(SQL)"

dashboard:          ## build the dashboard (+ dbt docs) and preview at http://localhost:8081
	$(PIPELINE) dashboard --with-docs --serve

docs:               ## generate the dbt documentation site (data/dbt/target)
	$(PIPELINE) docs

docs-serve:         ## regenerate, then browse docs and lineage at http://localhost:8080
	$(PIPELINE) docs --serve --port 8080

freshness:          ## check the raw catalogue is recent (warn > 2 days, error > 7)
	$(PIPELINE) dbt source freshness

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

clean-venv:         ## delete the virtualenv (e.g. to switch Python version)
	$(SYSTEM_PYTHON) scripts/dev.py clean-venv

# PulseLake
#
# `make help` lists everything. `make start` runs the whole stack.

VENV       := .venv
PYTHON     := $(VENV)/bin/python
PIP        := $(VENV)/bin/pip
DBT        := $(CURDIR)/$(VENV)/bin/dbt
STREAMLIT  := $(VENV)/bin/streamlit

.DEFAULT_GOAL := help
.PHONY: help setup start stop restart status logs ingest loop dbt dbt-test freshness dashboard test lint clean reset

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

setup:  ## Create the virtualenv and install dependencies
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@echo "Done. Next: make start"

start:  ## Start ingestion, the dbt refresh loop, and the dashboard
	@scripts/pulselake.sh start

stop:  ## Stop everything
	@scripts/pulselake.sh stop

restart:  ## Stop then start
	@scripts/pulselake.sh restart

status:  ## Show what is running and how fresh the data is
	@scripts/pulselake.sh status

logs:  ## Tail all three logs
	@scripts/pulselake.sh logs

# Individual pieces, for running one layer at a time while developing.

ingest:  ## Run a single ingestion cycle
	$(PYTHON) -m ingest.run_once

loop:  ## Run the ingestion loop in the foreground (Ctrl+C to stop)
	$(PYTHON) -m ingest.run_loop

dbt:  ## Build the dbt models and run the tests
	cd transform && $(DBT) build

dbt-test:  ## Run the dbt tests only
	cd transform && $(DBT) test

freshness:  ## Check whether the raw data is stale
	cd transform && $(DBT) source freshness

dashboard:  ## Run the Streamlit dashboard in the foreground
	$(STREAMLIT) run dashboard/app.py

test:  ## Run the Python unit tests (offline, no database needed)
	$(PYTHON) -m pytest

lint:  ## Shellcheck the runner script, the same check CI runs
	@command -v shellcheck >/dev/null 2>&1 || { \
		echo "shellcheck is not installed. brew install shellcheck"; exit 1; }
	@shellcheck --version | grep version
	shellcheck scripts/pulselake.sh
	@echo "clean"

clean:  ## Remove build artefacts, keeping the data
	rm -rf transform/target transform/logs .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +

reset:  ## Delete the database and logs, keeping the code
	@echo "This deletes data/pulselake.duckdb and every log. Ctrl+C to abort."
	@read -r _
	rm -f data/*.duckdb data/*.duckdb.wal logs/*.log
	@echo "Reset. Run make start to begin collecting again."

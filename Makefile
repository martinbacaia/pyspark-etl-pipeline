.PHONY: help install lint typecheck test test-fast generate pipeline benchmark clean docker-build docker-run

PYTHON ?= .venv/bin/python
ifeq ($(OS),Windows_NT)
	PYTHON := .venv/Scripts/python.exe
endif

help: ## list available targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "%-18s %s\n", $$1, $$2}'

install: ## install runtime + dev deps into .venv
	python -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install -e .

lint: ## ruff lint
	$(PYTHON) -m ruff check src tests scripts

typecheck: ## mypy
	$(PYTHON) -m mypy src

test: ## full test suite
	$(PYTHON) -m pytest

test-fast: ## skip e2e
	$(PYTHON) -m pytest -k "not e2e"

generate: ## generate synthetic raw data
	$(PYTHON) scripts/generate_data.py

pipeline: ## run end-to-end Bronze -> Silver -> Gold
	$(PYTHON) -m pipeline.cli run

benchmark: ## benchmark at multiple sizes (default 100k, 1M)
	$(PYTHON) scripts/benchmark.py --sizes 100000,1000000

clean: ## wipe all data dirs
	$(PYTHON) -m pipeline.cli clean --yes

docker-build:
	docker build -t pyspark-etl-pipeline:latest .

docker-run:
	docker run --rm -v $(PWD)/data:/app/data pyspark-etl-pipeline:latest pipeline

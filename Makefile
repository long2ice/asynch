#!make

# Load local .env file
-include .env
export

DIRS = asynch/ tests/ benchmark/
PY_DEBUG_OPTS = PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1

up:
	@poetry update

deps:
	@poetry install --extras compression --no-root

bench: deps
	@python3 benchmark/main.py

check:
	ruff format --check $(DIRS)
	ruff check $(DIRS)

lint:
	ruff format $(DIRS)
	ruff check --fix $(DIRS)

test:
	$(PY_DEBUG_OPTS) pytest

build: deps clean
	@poetry build

clean:
	@rm -rf ./dist

ci: check test

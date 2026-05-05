#!make

# Load local .env file
-include .env
export

DIRS = asynch/ tests/ benchmark/
PY_DEBUG_OPTS = PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1

up:
	poetry update

deps:
	poetry install --extras compression --no-root --with lint,test

bench: deps
	python3 benchmark/main.py

check:
	ruff format --check $(DIRS)
	ruff check $(DIRS)

lint:
	ruff format $(DIRS)
	ruff check --fix $(DIRS)

test:
	$(PY_DEBUG_OPTS) poetry run pytest

# Run only PEP 249 compliance tests (shows all failures, not just the first)
test-pep249:
	$(PY_DEBUG_OPTS) poetry run pytest tests/pep249/ -p no:randomly --no-header --tb=short --override-ini="addopts=-s -vvv"

# Run only SQLAlchemy compatibility tests
test-sqlalchemy:
	$(PY_DEBUG_OPTS) poetry run pytest tests/sqlalchemy/ -p no:randomly --no-header --tb=short --override-ini="addopts=-s -vvv"

# Run PEP 249 + SQLAlchemy tests together (TDD workflow — see all failures at once)
test-compat:
	$(PY_DEBUG_OPTS) poetry run pytest tests/pep249/ tests/sqlalchemy/ -p no:randomly --no-header --tb=short --override-ini="addopts=-s -vvv"

build: deps clean
	poetry build

clean:
	rm -rf ./dist

ci: check test

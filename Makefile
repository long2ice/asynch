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

# ClickHouse database management
db-up:
	docker compose up -d clickhouse

db-down:
	docker compose down

db-logs:
	docker compose logs -f clickhouse

db-status:
	docker compose ps clickhouse

db-reset:
	docker compose down
	docker volume rm asynch_clickhouse_data 2>/dev/null || true
	docker compose up -d clickhouse

# Connect to ClickHouse CLI (requires database to be running)
db-cli:
	docker exec -it asynch_clickhouse clickhouse-client --user default

ci: check test

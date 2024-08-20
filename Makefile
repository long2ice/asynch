#!make

# Load local .env file
-include .env
export

checkfiles = asynch/ tests/ benchmark/
py_debug_envvars = PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1

up:
	@poetry update

deps:
	@poetry install --extras compression --no-root

bench: deps
	@python3 benchmark/main.py

check: deps
	@black --check $(checkfiles)
	@ruff check $(checkfiles)

style: deps
	@isort -src $(checkfiles)
	@black $(checkfiles)
	@ruff check --fix $(checkfiles)

test: deps
	$(py_debug_envvars) pytest

build: deps clean
	@poetry build

clean:
	@rm -rf ./dist

ci: check test

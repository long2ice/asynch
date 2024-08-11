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

style: deps
	@isort -src $(checkfiles)
	@black $(checkfiles)
	@ruff check --fix $(checkfiles)

check: deps
	@black --check $(checkfiles)
	@ruff check $(checkfiles)

test: deps
	$(py_debug_envvars) pytest -s -vvv tests

build: deps clean
	@poetry build

clean:
	@rm -rf ./dist

ci: check test

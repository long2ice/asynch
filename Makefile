#!make

# Load local .env file
-include .env
export

checkfiles = asynch/ tests/ benchmark/
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

up:
	@poetry update

deps:
	@poetry install --extras compression --no-root

style: deps
	@isort -src $(checkfiles)
	@black $(black_opts) $(checkfiles)

check: deps
	@black --check $(checkfiles)
	@ruff $(checkfiles) --fix

test: deps
	$(py_warn) pytest

build: deps clean
	@poetry build

clean:
	@rm -rf ./dist

ci: check test

checkfiles = asynch/ tests/ benchmark/
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1

help:
	@echo "asynch development makefile"
	@echo
	@echo  "usage: make <target>"
	@echo  "Targets:"
	@echo  "    up			Ensure dev/test dependencies are updated"
	@echo  "    deps		Ensure dev/test dependencies are installed"
	@echo  "    check		Checks that build is sane"
	@echo  "    test		Runs all tests"
	@echo  "    style		Auto-formats the code"
	@echo  "    build		Build package"
	@echo  "    clean		Clean old build"

up:
	@poetry update

deps:
	@poetry install --no-root

style: deps
	@poetry run isort -src $(checkfiles)
	@poetry run black $(black_opts) $(checkfiles)

check: deps
	@poetry run black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@poetry run flake8 $(checkfiles)
	@poetry run bandit -x tests -r $(checkfiles) -s B107

test: deps
	$(py_warn) poetry run pytest

build: deps clean
	@poetry build

clean:
	@rm -rf ./dist

ci: check test

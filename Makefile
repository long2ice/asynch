#!make

# Load local .env file
-include .env
export

checkfiles = asynch/ tests/ benchmark/ scripts/ build_cython.py
py_warn = PYTHONDEVMODE=1 PYTHONTRACEMALLOC=1

up:
	@uv lock --upgrade
	$(MAKE) deps options=--frozen

deps:
	uv sync --all-groups --all-extras $(options)

_style:
	@ruff format $(checkfiles)
	@ruff check --fix $(checkfiles)
style: deps _style

_stubtest:
	@if ls asynch/**/*.pyi >/dev/null 2>&1; then \
		stubtest asynch --mypy-config-file pyproject.toml --allowlist stubtest_allowlist.txt \
			--ignore-missing-stub --ignore-disjoint-bases --ignore-positional-only; \
	fi

_check:
	@ruff format --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@ruff check $(checkfiles)
	@mypy asynch/
	$(MAKE) _stubtest
check: deps _check

stubs: deps
	@python scripts/gen_stubs.py
	$(MAKE) _stubtest

_test:
	$(py_warn) pytest
test: deps _test

clean:
	@rm -rf build dist
	@find asynch \( -name '*.so' -o -name '*.c' -o -name '*.html' \) -delete

build: clean
	@uv build

benchmark: deps
	@python -m benchmark.run_all

ci: deps _check _test

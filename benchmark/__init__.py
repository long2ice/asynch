"""Benchmark suite for asynch, compared against clickhouse-driver.

Scenarios:

1. select     - large result sets, one column type per case
2. insert     - batched bulk inserts
3. concurrent - many queries in flight at once (the async advantage)
4. pool       - connection pool acquire/release overhead

clickhouse-driver (the synchronous C-extension driver) is the reference
implementation the results are compared against.

Run everything with `make benchmark` or `python -m benchmark.run_all`;
individual scenarios with e.g. `python -m benchmark.select`.
"""

from os import environ

from asynch.proto import constants

CONNECTION_USER = environ.get("CLICKHOUSE_USER", default=constants.DEFAULT_USER)
CONNECTION_PASSWORD = environ.get("CLICKHOUSE_PASSWORD", default=constants.DEFAULT_PASSWORD)
CONNECTION_HOST = environ.get("CLICKHOUSE_HOST", default="127.0.0.1")
CONNECTION_PORT = environ.get("CLICKHOUSE_PORT", default=constants.DEFAULT_PORT)
CONNECTION_DB = environ.get("CLICKHOUSE_DB", default=constants.DEFAULT_DATABASE)
CONNECTION_DSN = environ.get(
    "CLICKHOUSE_DSN",
    default=(
        f"clickhouse://{CONNECTION_USER}:{CONNECTION_PASSWORD}"
        f"@{CONNECTION_HOST}:{CONNECTION_PORT}"
        f"/{CONNECTION_DB}"
    ),
)

# Test data configuration
ROWS = int(environ.get("BENCHMARK_ROWS", 500_000))
INSERT_ROWS = int(environ.get("BENCHMARK_INSERT_ROWS", 200_000))
BATCH_SIZE = 10_000
CONCURRENT_QUERIES = 100
POOL_SIZE = 10

# Measurement configuration
WARMUP_RUNS = 1  # Runs discarded before measuring (page cache, connection warmup)
MEASURED_RUNS = 3  # Measured runs; the fastest one is reported


def best_of(times: list[float]) -> float:
    return min(times)


def fmt_rate(rows: int, seconds: float) -> str:
    rate = rows / seconds
    if rate >= 1e6:
        return f"{rate / 1e6:.2f}M rows/s"
    return f"{rate / 1e3:.0f}K rows/s"

"""Large-result-set SELECT throughput, one column type per case."""

import asyncio
from time import perf_counter

from clickhouse_driver import Client

from benchmark import (
    CONNECTION_HOST,
    CONNECTION_PASSWORD,
    CONNECTION_PORT,
    CONNECTION_USER,
    MEASURED_RUNS,
    ROWS,
    WARMUP_RUNS,
    best_of,
)
from benchmark.setup import get_connection


def queries(rows: int) -> dict[str, str]:
    return {
        "Int64": f"SELECT number FROM numbers({rows})",
        "String": f"SELECT toString(number) AS s FROM numbers({rows})",
        "FixedString": f"SELECT toFixedString(toString(number % 1000), 16) FROM numbers({rows})",
        "Nullable(Int64)": f"SELECT nullIf(number, 5) AS n FROM numbers({rows})",
        "Date": f"SELECT toDate(number % 40000) AS d FROM numbers({rows})",
        "DateTime": f"SELECT toDateTime(number % 4000000000) AS d FROM numbers({rows})",
        "Float64": f"SELECT number / 3 AS f FROM numbers({rows})",
        "UUID": f"SELECT generateUUIDv4() AS u FROM numbers({rows})",
        "Decimal(10,2)": f"SELECT toDecimal64(number % 100000, 2) AS d FROM numbers({rows})",
        "LowCardinality(String)": (
            f"SELECT toLowCardinality(toString(number % 1000)) AS lc FROM numbers({rows})"
        ),
        "Array(Int64)": f"SELECT range(number % 10) AS a FROM numbers({rows})",
        "Map(String,Int64)": (
            f"SELECT map(toString(number % 10), number) AS m FROM numbers({rows})"
        ),
        "Tuple(Int64,String)": (
            f"SELECT tuple(number, toString(number)) AS t FROM numbers({rows})"
        ),
        "mixed": (
            f"SELECT number, toString(number) AS s, nullIf(number, 5) AS n, "
            f"range(number % 5) AS a FROM numbers({rows})"
        ),
    }


async def bench_asynch(query: str) -> float:
    conn = await get_connection()
    times = []
    async with conn.cursor() as cursor:
        for _ in range(WARMUP_RUNS):
            await cursor.execute(query)
            await cursor.fetchall()
        for _ in range(MEASURED_RUNS):
            start = perf_counter()
            await cursor.execute(query)
            rows = await cursor.fetchall()
            times.append(perf_counter() - start)
            assert rows
    await conn.close()
    return best_of(times)


def bench_clickhouse_driver(query: str) -> float:
    client = Client(
        host=CONNECTION_HOST,
        port=int(CONNECTION_PORT),
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
    )
    times = []
    for _ in range(WARMUP_RUNS):
        client.execute(query)
    for _ in range(MEASURED_RUNS):
        start = perf_counter()
        rows = client.execute(query)
        times.append(perf_counter() - start)
        assert rows
    client.disconnect()
    return best_of(times)


async def run() -> list[tuple[str, int, float, float]]:
    """Return (case, rows, asynch_seconds, driver_seconds) per query."""
    results = []
    for name, query in queries(ROWS).items():
        asynch_time = await bench_asynch(query)
        driver_time = bench_clickhouse_driver(query)
        results.append((name, ROWS, asynch_time, driver_time))
    return results


if __name__ == "__main__":
    from benchmark.run_all import print_select_table

    print_select_table(asyncio.run(run()))

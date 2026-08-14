"""Realistic workload: a wide events table with mixed column types.

Scenarios people actually run: exporting a large result set, an aggregation,
and a filtered slice - not one column type at a time.
"""

from __future__ import annotations

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

EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS test.events
(
    `user_id`     UInt64,
    `event_time`  DateTime,
    `event_type`  LowCardinality(String),
    `url`         String,
    `referrer`    Nullable(String),
    `duration_ms` UInt32,
    `tags`        Array(String),
    `props`       Map(String, String)
)
ENGINE = MergeTree
ORDER BY (user_id, event_time)
"""

EVENTS_POPULATE = f"""
INSERT INTO test.events
SELECT
    number % 100000,
    toDateTime('2026-01-01 00:00:00') + number % 86400,
    ['view', 'click', 'scroll', 'purchase'][number % 4 + 1],
    concat('https://example.com/page/', toString(number % 1000)),
    if(number % 5 = 0, NULL, concat('https://ref.example.com/', toString(number % 100))),
    number % 60000,
    arrayMap(x -> concat('tag', toString(x)), range(number % 4)),
    map(
        'country', ['US', 'CN', 'DE'][number % 3 + 1],
        'device', ['mobile', 'desktop'][number % 2 + 1]
    )
FROM numbers({ROWS})
"""

SCENARIOS = {
    f"export {ROWS // 1000}k events (8 mixed columns)": "SELECT * FROM test.events",
    "filtered slice (~1% of rows)": (
        "SELECT * FROM test.events WHERE event_type = 'purchase' AND duration_ms < 600"
    ),
    "aggregation (GROUP BY over all rows)": (
        "SELECT event_type, count() AS events, avg(duration_ms) AS avg_ms, "
        "uniq(user_id) AS users FROM test.events GROUP BY event_type ORDER BY events DESC"
    ),
}


async def prepare() -> None:
    conn = await get_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
        await cursor.execute("DROP TABLE IF EXISTS test.events")
        await cursor.execute(EVENTS_DDL)
        await cursor.execute(EVENTS_POPULATE)
    await conn.close()


async def cleanup() -> None:
    conn = await get_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test.events")
    await conn.close()


async def bench_asynch(query: str) -> tuple[float, int]:
    conn = await get_connection()
    times = []
    async with conn.cursor() as cursor:
        for _ in range(WARMUP_RUNS):
            await cursor.execute(query)
            rows = await cursor.fetchall()
        for _ in range(MEASURED_RUNS):
            start = perf_counter()
            await cursor.execute(query)
            rows = await cursor.fetchall()
            times.append(perf_counter() - start)
    await conn.close()
    return best_of(times), len(rows)


def bench_clickhouse_driver(query: str) -> tuple[float, int]:
    client = Client(
        host=CONNECTION_HOST,
        port=int(CONNECTION_PORT),
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
    )
    times = []
    for _ in range(WARMUP_RUNS):
        rows = client.execute(query)
    for _ in range(MEASURED_RUNS):
        start = perf_counter()
        rows = client.execute(query)
        times.append(perf_counter() - start)
    client.disconnect()
    return best_of(times), len(rows)


async def run() -> list[tuple[str, int, float, float]]:
    await prepare()
    results = []
    for name, query in SCENARIOS.items():
        asynch_time, n_rows = await bench_asynch(query)
        driver_time, _ = bench_clickhouse_driver(query)
        results.append((name, n_rows, asynch_time, driver_time))
    await cleanup()
    return results


if __name__ == "__main__":
    from benchmark.run_all import print_time_table

    print_time_table(asyncio.run(run()), title="Realistic workload")

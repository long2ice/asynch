"""Batched bulk-insert throughput."""

import asyncio
from datetime import datetime
from time import perf_counter

from clickhouse_driver import Client

from benchmark import (
    BATCH_SIZE,
    CONNECTION_HOST,
    CONNECTION_PASSWORD,
    CONNECTION_PORT,
    CONNECTION_USER,
    INSERT_ROWS,
    MEASURED_RUNS,
    WARMUP_RUNS,
    best_of,
)
from benchmark.setup import get_connection, setup_database

INSERT_SQL = "INSERT INTO test.benchmark (id, name, value, created, tags) VALUES"


def make_batch(size: int) -> list[tuple]:
    now = datetime(2026, 1, 1, 12, 0, 0)
    return [(i, f"name-{i % 1000}", i / 3.0, now, [f"tag{i % 5}", "common"]) for i in range(size)]


async def bench_asynch() -> float:
    batch = make_batch(BATCH_SIZE)
    conn = await get_connection()
    times = []
    async with conn.cursor() as cursor:
        for run_index in range(WARMUP_RUNS + MEASURED_RUNS):
            await cursor.execute("TRUNCATE TABLE test.benchmark")
            start = perf_counter()
            for _ in range(INSERT_ROWS // BATCH_SIZE):
                await cursor.execute(INSERT_SQL, batch)
            elapsed = perf_counter() - start
            if run_index >= WARMUP_RUNS:
                times.append(elapsed)
    await conn.close()
    return best_of(times)


def bench_clickhouse_driver() -> float:
    batch = make_batch(BATCH_SIZE)
    client = Client(
        host=CONNECTION_HOST,
        port=int(CONNECTION_PORT),
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
    )
    times = []
    for run_index in range(WARMUP_RUNS + MEASURED_RUNS):
        client.execute("TRUNCATE TABLE test.benchmark")
        start = perf_counter()
        for _ in range(INSERT_ROWS // BATCH_SIZE):
            client.execute(INSERT_SQL, batch)
        elapsed = perf_counter() - start
        if run_index >= WARMUP_RUNS:
            times.append(elapsed)
    client.disconnect()
    return best_of(times)


async def run() -> list[tuple[str, int, float, float]]:
    await setup_database()
    asynch_time = await bench_asynch()
    driver_time = bench_clickhouse_driver()
    return [("batch insert", INSERT_ROWS, asynch_time, driver_time)]


if __name__ == "__main__":
    from benchmark.run_all import print_select_table

    print_select_table(asyncio.run(run()))

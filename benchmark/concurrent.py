"""Many queries in flight at once - where an async driver should shine.

asynch runs CONCURRENT_QUERIES point-lookup-style queries through a pool with
POOL_SIZE connections; clickhouse-driver runs the same queries sequentially on
one connection (its idiomatic usage: it has no pool and blocks per call).
"""

import asyncio
from time import perf_counter

from clickhouse_driver import Client

from asynch import Pool
from benchmark import (
    CONCURRENT_QUERIES,
    CONNECTION_DSN,
    CONNECTION_HOST,
    CONNECTION_PASSWORD,
    CONNECTION_PORT,
    CONNECTION_USER,
    MEASURED_RUNS,
    POOL_SIZE,
    WARMUP_RUNS,
    best_of,
)

QUERY = "SELECT sum(number) FROM numbers(100000)"


async def bench_asynch() -> float:
    async with Pool(dsn=CONNECTION_DSN, minsize=POOL_SIZE, maxsize=POOL_SIZE) as pool:

        async def one_query() -> None:
            async with pool.connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(QUERY)
                    await cursor.fetchall()

        times = []
        for run_index in range(WARMUP_RUNS + MEASURED_RUNS):
            start = perf_counter()
            await asyncio.gather(*(one_query() for _ in range(CONCURRENT_QUERIES)))
            elapsed = perf_counter() - start
            if run_index >= WARMUP_RUNS:
                times.append(elapsed)
    return best_of(times)


def bench_clickhouse_driver() -> float:
    client = Client(
        host=CONNECTION_HOST,
        port=int(CONNECTION_PORT),
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
    )
    times = []
    for run_index in range(WARMUP_RUNS + MEASURED_RUNS):
        start = perf_counter()
        for _ in range(CONCURRENT_QUERIES):
            client.execute(QUERY)
        elapsed = perf_counter() - start
        if run_index >= WARMUP_RUNS:
            times.append(elapsed)
    client.disconnect()
    return best_of(times)


async def run() -> list[tuple[str, int, float, float]]:
    asynch_time = await bench_asynch()
    driver_time = bench_clickhouse_driver()
    return [
        (
            f"{CONCURRENT_QUERIES} queries, pool of {POOL_SIZE}",
            CONCURRENT_QUERIES,
            asynch_time,
            driver_time,
        )
    ]


if __name__ == "__main__":
    from benchmark.run_all import print_ops_table

    print_ops_table(asyncio.run(run()), unit="queries")

"""Pool acquire/release overhead: cost of `async with pool.connection()`."""

from __future__ import annotations

import asyncio
from time import perf_counter

from asynch import Pool
from benchmark import CONNECTION_DSN, MEASURED_RUNS, POOL_SIZE, WARMUP_RUNS, best_of

ACQUISITIONS = 2_000


async def bench_acquire_release() -> float:
    async with Pool(dsn=CONNECTION_DSN, minsize=POOL_SIZE, maxsize=POOL_SIZE) as pool:
        times = []
        for run_index in range(WARMUP_RUNS + MEASURED_RUNS):
            start = perf_counter()
            for _ in range(ACQUISITIONS):
                async with pool.connection():
                    pass
            elapsed = perf_counter() - start
            if run_index >= WARMUP_RUNS:
                times.append(elapsed)
    return best_of(times)


async def run() -> list[tuple[str, int, float, float | None]]:
    elapsed = await bench_acquire_release()
    return [("pool acquire/release", ACQUISITIONS, elapsed, None)]


if __name__ == "__main__":
    from benchmark.run_all import print_ops_table

    print_ops_table(asyncio.run(run()), unit="acquisitions")

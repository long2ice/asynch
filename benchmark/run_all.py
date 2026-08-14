"""Run all benchmarks and print a report.

Usage: python -m benchmark.run_all
Requires a running ClickHouse (CLICKHOUSE_* env vars, defaults to localhost:9000).
"""

from __future__ import annotations

import asyncio
import platform
import sys

from rich.console import Console
from rich.table import Table

from benchmark import INSERT_ROWS, MEASURED_RUNS, ROWS, fmt_rate
from benchmark.setup import cleanup_database

console = Console()


def _ratio(asynch_time: float, driver_time: float | None) -> str:
    if driver_time is None:
        return "-"
    ratio = driver_time / asynch_time
    if ratio >= 1:
        return f"[green]{ratio:.2f}x faster[/green]"
    return f"[red]{1 / ratio:.2f}x slower[/red]"


def print_select_table(results, title="SELECT throughput"):
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Case", style="yellow")
    table.add_column("asynch", justify="right")
    table.add_column("clickhouse-driver", justify="right")
    table.add_column("asynch vs driver", justify="right")
    for name, rows, asynch_time, driver_time in results:
        table.add_row(
            name,
            fmt_rate(rows, asynch_time),
            fmt_rate(rows, driver_time) if driver_time is not None else "-",
            _ratio(asynch_time, driver_time),
        )
    console.print(table)


def print_time_table(results, title="Elapsed time"):
    """For task-shaped scenarios where elapsed time is the intuitive unit."""
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Scenario", style="yellow")
    table.add_column("Rows", justify="right")
    table.add_column("asynch", justify="right")
    table.add_column("clickhouse-driver", justify="right")
    table.add_column("asynch vs driver", justify="right")
    for name, rows, asynch_time, driver_time in results:
        table.add_row(
            name,
            f"{rows:,}",
            f"{asynch_time * 1000:,.0f} ms",
            f"{driver_time * 1000:,.0f} ms" if driver_time is not None else "-",
            _ratio(asynch_time, driver_time),
        )
    console.print(table)


def print_ops_table(results, unit="ops", title=None):
    table = Table(title=title, show_header=True, header_style="bold cyan")
    table.add_column("Case", style="yellow")
    table.add_column(f"asynch ({unit}/s)", justify="right")
    table.add_column(f"clickhouse-driver ({unit}/s)", justify="right")
    table.add_column("asynch vs driver", justify="right")
    for name, ops, asynch_time, driver_time in results:
        table.add_row(
            name,
            f"{ops / asynch_time:,.0f}",
            f"{ops / driver_time:,.0f}" if driver_time is not None else "-",
            _ratio(asynch_time, driver_time),
        )
    console.print(table)


async def main() -> None:
    from benchmark import concurrent, insert, pool, realistic, select

    console.print(
        f"[bold]asynch benchmark[/bold] - Python {platform.python_version()}, "
        f"{platform.machine()}, best of {MEASURED_RUNS} runs\n"
        f"SELECT rows: {ROWS:,}  INSERT rows: {INSERT_ROWS:,}\n"
    )

    console.print("[dim]running realistic workload...[/dim]")
    realistic_results = await realistic.run()
    print_time_table(realistic_results, title="Realistic workload (wide events table)")

    console.print("\n[dim]running per-column-type SELECT benchmarks...[/dim]")
    select_results = await select.run()
    print_select_table(select_results, title="Column-type micro-benchmarks (SELECT)")

    console.print("\n[dim]running INSERT benchmark...[/dim]")
    insert_results = await insert.run()
    print_select_table(insert_results, title="INSERT throughput")

    console.print("\n[dim]running concurrency benchmark...[/dim]")
    concurrent_results = await concurrent.run()
    print_ops_table(
        concurrent_results,
        unit="queries",
        title="Concurrent queries (pool vs sequential driver)",
    )

    console.print("\n[dim]running pool benchmark...[/dim]")
    pool_results = await pool.run()
    print_ops_table(pool_results, unit="acquisitions", title="Pool overhead")

    await cleanup_database()

    scored = realistic_results + select_results + insert_results
    wins = sum(1 for _, _, a, d in scored if d is not None and a <= d)
    total = sum(1 for _, _, _, d in scored if d is not None)
    console.print(
        f"\n[bold]asynch matches or beats clickhouse-driver in {wins}/{total} cases[/bold]"
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)

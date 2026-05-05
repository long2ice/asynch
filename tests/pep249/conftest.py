"""
Fixtures for PEP 249 compliance tests.

These tests run against a live ClickHouse instance.  Connection settings are
inherited from the root conftest via the `config` fixture.

A dedicated table `test.pep249` is created once per test session and truncated
before each test function.
"""

import pytest

from asynch.connection import Connection
from asynch.cursors import Cursor

PEP249_TABLE = "test.pep249"
PEP249_DDL = f"""
    CREATE TABLE IF NOT EXISTS {PEP249_TABLE}
    (
        id       Int32,
        name     Nullable(String),
        value    Float64,
        created  Date,
        updated  Nullable(DateTime),
        flag     Bool
    )
    ENGINE = MergeTree
    ORDER BY id
"""


@pytest.fixture(scope="session", autouse=True)
async def pep249_table(config):
    """Create the pep249 test table once for the entire session."""
    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
            await cursor.execute(f"DROP TABLE IF EXISTS {PEP249_TABLE}")
            await cursor.execute(PEP249_DDL)
    yield
    # teardown: drop the table after the session
    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {PEP249_TABLE}")


@pytest.fixture(autouse=True)
async def truncate_pep249(config):
    """Truncate pep249 test table before each test."""
    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"TRUNCATE TABLE {PEP249_TABLE}")
    yield


@pytest.fixture
async def pep249_conn(config) -> Connection:
    """Open connection for the duration of a single test."""
    async with Connection(dsn=config.dsn) as conn:
        yield conn


@pytest.fixture
async def pep249_cursor(pep249_conn) -> Cursor:
    """Open cursor for the duration of a single test."""
    async with pep249_conn.cursor() as cursor:
        yield cursor


@pytest.fixture
async def populated_table(pep249_conn, config):
    """Insert a handful of rows into pep249 and return their data."""
    import datetime

    rows = [
        (1, "Alice", 1.5, datetime.date(2024, 1, 1), datetime.datetime(2024, 1, 1, 12, 0, 0), True),
        (2, "Bob", 2.5, datetime.date(2024, 1, 2), None, False),
        (3, None, 3.5, datetime.date(2024, 1, 3), datetime.datetime(2024, 1, 3, 9, 0, 0), True),
    ]
    async with pep249_conn.cursor() as cursor:
        await cursor.executemany(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, updated, flag) VALUES",
            rows,
        )
    return rows

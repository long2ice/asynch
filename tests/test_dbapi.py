"""PEP 249 module-level surface."""

import pytest

import asynch


def test_pep249_module_globals():
    assert asynch.apilevel == "2.0"
    # 1: threads may share the module, but not connections - a Connection
    # owns a single socket and parse buffer.
    assert asynch.threadsafety == 1
    assert asynch.paramstyle == "pyformat"


def test_pep249_connect_and_exceptions():
    conn = asynch.connect(dsn="clickhouse://127.0.0.1:9000")
    assert isinstance(conn, asynch.Connection)
    # PEP 249 requires the exception hierarchy on the module.
    assert issubclass(asynch.OperationalError, asynch.DatabaseError)
    assert issubclass(asynch.DatabaseError, asynch.Error)


@pytest.mark.asyncio
async def test_cursor_arraysize(conn):
    async with conn.cursor() as cursor:
        assert cursor.arraysize == 1
        cursor.arraysize = 3
        assert cursor.arraysize == 3
        await cursor.execute("SELECT number FROM system.numbers LIMIT 10")
        assert len(await cursor.fetchmany(None)) == 3

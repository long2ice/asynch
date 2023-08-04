import re
from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from asynch.proto.connection import Connection
from conftest import (
    CONNECTION_DB,
    CONNECTION_HOST,
    CONNECTION_PASSWORD,
    CONNECTION_PORT,
    CONNECTION_USER,
)


@pytest.fixture()
async def conn() -> Connection:
    _conn = Connection(
        host=CONNECTION_HOST,
        port=CONNECTION_PORT,
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
        database=CONNECTION_DB,
    )
    await _conn.connect()
    yield _conn
    await _conn.disconnect()


@pytest.mark.asyncio
async def test_connect(conn: Connection):
    assert conn.connected
    assert conn.server_info.name == "ClickHouse"
    assert conn.server_info.timezone == "UTC"
    assert re.match(r"\w+", conn.server_info.display_name)
    assert isinstance(conn.server_info.version_patch, int)


@pytest.mark.asyncio
async def test_ping(conn: Connection):
    await conn.connect()
    assert await conn.ping() is True


@pytest.mark.asyncio
async def test_ping_processing_with_invalid_package_size(conn: Connection):
    with patch.object(
        conn.reader, "_read_one", side_effect=IndexError("Empty bytes array")
    ) as mock:
        result = await conn.ping()
        mock.assert_called_once()
        assert result is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(ConnectionError("Any connection error"), id="any ConnectionError"),
        pytest.param(OSError("Any OS error"), id="any OSError"),
        pytest.param(
            RuntimeError(
                "RuntimeError: TCPTransport closed=True: localhost"
            ),  # Check parsing exc message
            id="RuntimeError with TCPTransport closed",
        ),
    ],
)
async def test_ping_catch_connection_error(conn: Connection, exception: Exception):
    with patch.object(conn.reader, "read_varint", side_effect=exception) as mock:
        result = await conn.ping()
        mock.assert_called_once()
        assert result is False


@pytest.mark.asyncio
async def test_ping_raise_other_runtime_errors(conn: Connection):
    with patch.object(
        conn.reader, "read_varint", side_effect=RuntimeError("Any exception")
    ) as mock:
        with pytest.raises(RuntimeError, match="Any exception"):
            await conn.ping()
        mock.assert_called_once()


@pytest.mark.asyncio
async def test_execute(conn: Connection):
    query = "SELECT 1"
    ret = await conn.execute(query)
    assert ret == [(1,)]


@asynccontextmanager
async def create_table(connection, spec):
    await connection.execute("DROP TABLE IF EXISTS test.test")
    await connection.execute(f"CREATE TABLE test.test ({spec}) engine=Memory")

    try:
        yield
    finally:
        await connection.execute("DROP TABLE test.test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, data, expected",
    [
        ("a Int8, b String", [(None, None)], [(0, "")]),
        ("a LowCardinality(String)", [(None,)], [("",)]),
        ("a Tuple(Int32, Int32)", [(None,)], [((0, 0),)]),
        ("a Array(Array(Int32))", [(None,)], [([],)]),
        ("a Map(String, UInt64)", [(None,)], [({},)]),
        ("a Nested(i Int32)", [(None,)], [([],)]),
    ],
    ids=[
        "int and string",
        "lowcardinaly string",
        "tuple",
        "array",
        "map",
        "nested",
    ],
)
async def test_input_format_null_as_default(conn, spec, data, expected):
    for enabled in (True, False):
        conn.client_settings["input_format_null_as_default"] = enabled

        async with create_table(conn, spec):
            try:
                await conn.execute("INSERT INTO test.test VALUES", data)
            except:  # noqa
                assert not enabled
                return

            assert await conn.execute("SELECT * FROM test.test") == expected


@pytest.mark.asyncio
async def test_watch_zero_limit(conn: Connection) -> None:
    await conn.execute("DROP TABLE IF EXISTS test.test")
    await conn.execute("CREATE TABLE test.test (x Int8) ENGINE=Memory;")
    await conn.execute("SET allow_experimental_live_view = 1")
    await conn.execute("DROP VIEW IF EXISTS lv")
    await conn.execute("CREATE LIVE VIEW lv AS SELECT sum(x) FROM test.test")
    await conn.execute("INSERT INTO test.test VALUES (10)")
    iter = await conn.execute_iter("WATCH lv LIMIT 0")
    async for data in iter:
        assert data == (10, 1)

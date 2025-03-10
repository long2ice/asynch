import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import cast
from unittest.mock import patch

import pytest

from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.cs import ServerInfo


@pytest.fixture()
async def proto_conn(config) -> AsyncIterator[ProtoConnection]:
    _conn = ProtoConnection(
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database,
    )
    await _conn.connect()
    yield _conn
    await _conn.disconnect()


@pytest.mark.asyncio
async def test_connect(proto_conn: ProtoConnection):
    assert proto_conn.connected

    server_info = cast(ServerInfo, proto_conn.server_info)
    assert server_info.name == "ClickHouse"
    assert server_info.timezone == "UTC"
    assert re.match(r"\w+", server_info.display_name)
    assert isinstance(server_info.version_patch, int)


@pytest.mark.asyncio
async def test_ping(proto_conn: ProtoConnection):
    await proto_conn.connect()
    assert await proto_conn.ping() is True


@pytest.mark.asyncio
async def test_ping_processing_with_invalid_package_size(proto_conn: ProtoConnection):
    with patch.object(
        proto_conn.reader, "_read_one", side_effect=IndexError("Empty bytes array")
    ) as mock:
        result = await proto_conn.ping()
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
async def test_ping_catch_connection_error(proto_conn: ProtoConnection, exception: Exception):
    with patch.object(proto_conn.reader, "read_varint", side_effect=exception) as mock:
        result = await proto_conn.ping()
        mock.assert_called_once()
        assert result is False


@pytest.mark.asyncio
async def test_ping_raise_other_runtime_errors(proto_conn: ProtoConnection):
    with patch.object(
        proto_conn.reader, "read_varint", side_effect=RuntimeError("Any exception")
    ) as mock:
        with pytest.raises(RuntimeError, match="Any exception"):
            await proto_conn.ping()
        mock.assert_called_once()


@pytest.mark.asyncio
async def test_execute(proto_conn: ProtoConnection):
    query = "SELECT 1"
    ret = await proto_conn.execute(query)
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
async def test_input_format_null_as_default(proto_conn, spec, data, expected):
    for enabled in (True, False):
        proto_conn.client_settings["input_format_null_as_default"] = enabled

        async with create_table(proto_conn, spec):
            try:
                await proto_conn.execute("INSERT INTO test.test VALUES", data)
            except:  # noqa
                assert not enabled
                return

            assert await proto_conn.execute("SELECT * FROM test.test") == expected


@pytest.mark.asyncio
async def test_watch_zero_limit(proto_conn: ProtoConnection) -> None:
    await proto_conn.execute("DROP TABLE IF EXISTS test.test")
    await proto_conn.execute("CREATE TABLE test.test (x Int8) ENGINE=Memory;")
    await proto_conn.execute("SET allow_experimental_live_view = 1")
    await proto_conn.execute("DROP VIEW IF EXISTS lv")
    await proto_conn.execute("CREATE LIVE VIEW lv AS SELECT sum(x) FROM test.test")
    await proto_conn.execute("INSERT INTO test.test VALUES (10)")
    iter = await proto_conn.execute_iter("WATCH lv LIMIT 0")
    async for data in iter:
        assert data == (10, 1)

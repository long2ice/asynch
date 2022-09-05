import re
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
    return _conn


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

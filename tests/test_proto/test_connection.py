import re

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
async def test_execute(conn: Connection):
    query = "SELECT 1"
    ret = await conn.execute(query)
    assert ret == [(1,)]

import pytest

from conftest import conn


@pytest.mark.asyncio
async def test_connect():
    assert conn.connected
    assert conn.server_info.name == "ClickHouse"
    assert conn.server_info.timezone == "UTC"
    assert conn.server_info.display_name == "de20414d19b9"
    assert conn.server_info.version_patch == 2


@pytest.mark.asyncio
async def test_ping():
    assert await conn.ping()


@pytest.mark.asyncio
async def test_send_query():
    query = "SELECT 1"
    await conn.send_query(query)

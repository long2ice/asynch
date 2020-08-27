import pytest

from asynch.connection import Connection


@pytest.mark.asyncio
async def test_send_ping():
    conn = Connection()
    await conn.connect()
    # await conn.ping()

import pytest

from asynch import Connection

conn = Connection()


@pytest.mark.asyncio
async def test_execute():
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT * FROM synch.log")
        ret = cursor.fetchone()
        assert ret == (1,)

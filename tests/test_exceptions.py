import pytest

from asynch import Connection
from asynch.errors import ServerException
from asynch.pool import Pool


@pytest.mark.asyncio
async def test_database_exists(config):
    async with Pool(dsn=config.dsn) as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                with pytest.raises(ServerException):
                    await cursor.execute("create database test")


@pytest.mark.asyncio
async def test_connection_does_not_close_after_exception():
    async with Connection() as conn:
        async with conn.cursor() as cur:
            with pytest.raises(ServerException):
                await cur.execute("foo")

            assert conn._connection.connected is True
            assert conn.opened is True

            await cur.execute("select 1")

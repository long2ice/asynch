import pytest

from asynch.errors import ServerException


@pytest.mark.asyncio
async def test_database_exists(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            with pytest.raises(ServerException):
                await cursor.execute("create database test")

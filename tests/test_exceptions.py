import pytest

from asynch.errors import ServerException
from asynch.pool import Pool


@pytest.mark.asyncio
async def test_database_exists(config):
    async with Pool(dsn=config.dsn) as pool:
        conn = await pool.connection()
        async with conn.cursor() as cursor:
            with pytest.raises(ServerException):
                await cursor.execute("create database test")

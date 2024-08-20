import pytest

from asynch.connection import Connection


@pytest.mark.asyncio
async def test_compress_lz4(dsn):
    async with Connection(dsn=dsn.dsn, compression=True) as conn_lz4:
        async with conn_lz4.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1


@pytest.mark.asyncio
async def test_compress_lz4hc(dsn):
    async with Connection(dsn=dsn.dsn, compression="lz4hc") as conn_lz4hc:
        async with conn_lz4hc.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1


@pytest.mark.asyncio
async def test_compress_zstd(dsn):
    async with Connection(dsn=dsn.dsn, compression="zstd") as conn_zstd:
        async with conn_zstd.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1

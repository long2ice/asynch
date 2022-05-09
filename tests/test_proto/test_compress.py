import pytest

from asynch.proto.connection import Connection


@pytest.mark.asyncio
async def test_compress_lz4():
    connection = Connection(compression=True)
    await connection.connect()
    ret = await connection.execute("SELECT 1")
    assert ret == [(1,)]


@pytest.mark.asyncio
async def test_compress_lz4hc():
    connection = Connection(compression="lz4hc")
    await connection.connect()
    ret = await connection.execute("SELECT 1")
    assert ret == [(1,)]


@pytest.mark.asyncio
async def test_compress_zstd():
    connection = Connection(compression="zstd")
    await connection.connect()
    ret = await connection.execute("SELECT 1")
    assert ret == [(1,)]

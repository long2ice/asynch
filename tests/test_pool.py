from asyncio import gather
from socket import gaierror

import pytest

from asynch.connection import Connection


@pytest.mark.asyncio
async def test_pool(pool):
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.asyncio
async def test_pool_cursor(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            ret = await cursor.fetchone()
            assert ret == (1,)


@pytest.mark.asyncio
async def test_acquire(pool):
    conn = await pool.acquire()
    assert isinstance(conn, Connection)
    assert pool.freesize == 0
    assert pool.size == 1
    assert conn.connected
    await pool.release(conn)
    assert pool.freesize == 1
    assert pool.size == 1


@pytest.mark.asyncio
async def test_clean_pool_connection_on_error(pool):
    assert pool

    async def raise_net_error():
        raise gaierror

    for _ in range(100):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    # simulate network error while executing the query
                    await gather(cursor.execute("SELECT 1"), raise_net_error())
                except gaierror:
                    pass

    assert conn._connection.writer is None
    assert conn._connection.reader is None
    assert conn._connection.block_reader is None
    assert conn._connection.block_reader_raw is None
    assert conn._connection.block_writer is None
    assert conn._connection.connected is None
    assert conn._connection.client_trace_context is None
    assert conn._connection.server_info is None
    assert conn._connection.is_query_executing is False

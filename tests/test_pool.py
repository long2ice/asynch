import asyncio
from typing import Any

import pytest

from asynch.connection import Connection
from asynch.pool import Pool, PoolError
from asynch.proto import constants
from asynch.proto.models.enums import PoolStatuses


@pytest.mark.asyncio
async def test_pool_size_boundary_values():
    """If not marked as asyncio, then `RuntimeError: no running event loop` occurs."""

    Pool(minsize=0)
    with pytest.raises(ValueError, match=r"minsize is expected to be greater or equal to zero"):
        Pool(minsize=-1)

    Pool(minsize=0, maxsize=1)
    with pytest.raises(ValueError, match=r"maxsize is expected to be greater than zero"):
        Pool(maxsize=0)

    Pool(minsize=1, maxsize=1)
    with pytest.raises(ValueError, match=r"minsize is greater than maxsize"):
        Pool(minsize=2, maxsize=1)


@pytest.mark.asyncio
async def test_pool_repr():
    pool = Pool()
    repstr = (
        f"<Pool(minsize={constants.POOL_MIN_SIZE}, maxsize={constants.POOL_MAX_SIZE})"
        f" object at 0x{id(pool):x}; status: {PoolStatuses.created}>"
    )
    assert repr(pool) == repstr

    min_size, max_size = 2, 3
    pool = Pool(minsize=min_size, maxsize=max_size)
    async with pool:
        repstr = (
            f"<Pool(minsize={min_size}, maxsize={max_size}) "
            f"object at 0x{id(pool):x}; status: {PoolStatuses.opened}>"
        )
        assert repr(pool) == repstr

    repstr = (
            f"<Pool(minsize={min_size}, maxsize={max_size}) "
            f"object at 0x{id(pool):x}; status: {PoolStatuses.closed}>"
        )
    assert repr(pool) == repstr


@pytest.mark.asyncio
async def test_pool_connection_attributes(config):
    pool = Pool(dsn=config.dsn)
    assert pool.minsize == constants.POOL_MIN_SIZE
    assert pool.maxsize == constants.POOL_MAX_SIZE
    assert pool.connections == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0

    async with pool:
        assert pool.connections == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

        async with pool.connection():
            assert pool.connections == constants.POOL_MIN_SIZE
            assert pool.free_connections == 0
            assert pool.acquired_connections == constants.POOL_MIN_SIZE

        assert pool.connections == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

    assert pool.connections == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0


@pytest.mark.asyncio
async def test_pool_connection_management(get_tcp_connections):
    """Tests connection cleanup when leaving a pool context.

    No dangling/unclosed connections must leave behind.
    """

    conn = Connection()
    init_tcps: int = await get_tcp_connections(conn)

    min_size, max_size = 1, 2
    async with Pool(minsize=min_size, maxsize=max_size) as pool:
        async with pool.connection():
            assert pool.free_connections == 0
            assert pool.acquired_connections == min_size
        assert pool.free_connections == min_size
        assert pool.acquired_connections == 0

        async with pool.connection() as cn1:
            async with pool.connection() as cn2:
                assert pool.free_connections == 0
                assert pool.acquired_connections == max_size

                # cannot acquire more than pool.maxsize
                with pytest.raises(PoolError):
                    async with pool.connection():
                        pass

                # the returned connections are functional
                async with cn1.cursor() as cur:
                    await cur.execute("SELECT 21")
                    ret = await cur.fetchone()
                    assert ret == (21,)
                async with cn2.cursor() as cur:
                    await cur.execute("SELECT 42")
                    ret = await cur.fetchone()
                    assert ret == (42,)

                # the status quo has remained
                assert pool.free_connections == 0
                assert pool.acquired_connections == max_size

            assert pool.free_connections == 1
            assert pool.acquired_connections == 1

        assert pool.free_connections == max_size
        assert pool.acquired_connections == 0

    assert init_tcps == await get_tcp_connections(conn)
    await conn.close()


@pytest.mark.asyncio
async def test_pool_reuse(get_tcp_connections):
    """Tests connection pool reusability."""

    async def _test_pool(pool: Pool):
        async with pool.connection() as cn1_ctx:
            assert pool.acquired_connections == pool.minsize
            assert pool.free_connections == 0
            assert pool.connections == pool.minsize

            async with pool.connection() as cn2_ctx:
                async with cn1_ctx.cursor() as cur:
                    await cur.execute("SELECT 21")
                    ret = await cur.fetchone()
                    assert ret == (21,)
                async with cn2_ctx.cursor() as cur:
                    await cur.execute("SELECT 42")
                    ret = await cur.fetchone()
                    assert ret == (42,)
                assert pool.acquired_connections == pool.maxsize
                assert pool.free_connections == 0
                assert pool.connections == pool.maxsize

            assert pool.acquired_connections == pool.minsize
            assert pool.free_connections == pool.minsize
            assert pool.connections == pool.maxsize

    conn = Connection()
    init_tcps: int = await get_tcp_connections(conn)

    min_size, max_size = 1, 2
    pool = Pool(minsize=min_size, maxsize=max_size)

    for _ in range(2):
        async with pool:
            await _test_pool(pool)
        assert await get_tcp_connections(conn) <= init_tcps
    await conn.close()


@pytest.mark.asyncio
async def test_pool_concurrent_connection_management(get_tcp_connections):
    """Tests pool connection managements on concurrent connections.

    A pool must not be broken when connections are acquired from concurrent tasks.
    When leaving the pool, all acquired connections become invalidated.
    No dangling/unclosed connections must remain.
    """

    async def _test_pool_connection(pool: Pool, *, selectee: Any = 42):
        async with pool.connection() as conn_ctx:
            async with conn_ctx.cursor() as cur:
                await cur.execute(f"SELECT {selectee}")
                ret = await cur.fetchone()
                assert ret == (selectee,)
                return selectee

    conn = Connection()
    init_tcps: int = await get_tcp_connections(conn)

    min_size, max_size = 10, 21
    selectees = list(range(min_size, max_size))
    answers: list[int] = []
    async with Pool(minsize=min_size, maxsize=max_size) as pool:
        tasks: list[asyncio.Task] = [
            asyncio.create_task(_test_pool_connection(pool=pool, selectee=selectee))
            for selectee in selectees
        ]
        answers = await asyncio.gather(*tasks)

    assert await get_tcp_connections(conn) <= init_tcps
    await conn.close()

    assert selectees == answers

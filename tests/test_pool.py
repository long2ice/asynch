import asyncio
from typing import Any

import pytest

from asynch.connection import Connection
from asynch.errors import AsynchPoolError
from asynch.pool import Pool
from asynch.proto import constants
from asynch.proto.models.enums import PoolStatus


def _get_pool_size(pool: Pool) -> int:
    return pool.acquired_connections + pool.free_connections


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
        f" object at 0x{id(pool):x}; status: {PoolStatus.created}>"
    )
    assert repr(pool) == repstr

    min_size, max_size = 2, 3
    pool = Pool(minsize=min_size, maxsize=max_size)
    async with pool:
        repstr = (
            f"<Pool(minsize={min_size}, maxsize={max_size}) "
            f"object at 0x{id(pool):x}; status: {PoolStatus.opened}>"
        )
        assert repr(pool) == repstr

    repstr = (
        f"<Pool(minsize={min_size}, maxsize={max_size}) "
        f"object at 0x{id(pool):x}; status: {PoolStatus.closed}>"
    )
    assert repr(pool) == repstr


@pytest.mark.asyncio
async def test_pool_connection_attributes(config):
    pool = Pool(dsn=config.dsn)
    assert pool.minsize == constants.POOL_MIN_SIZE
    assert pool.maxsize == constants.POOL_MAX_SIZE
    assert _get_pool_size(pool) == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0

    async with pool:
        assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

        async with pool.connection():
            assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
            assert pool.free_connections == 0
            assert pool.acquired_connections == constants.POOL_MIN_SIZE

        assert _get_pool_size(pool) == constants.POOL_MIN_SIZE
        assert pool.free_connections == constants.POOL_MIN_SIZE
        assert pool.acquired_connections == 0

    assert _get_pool_size(pool) == 0
    assert pool.free_connections == 0
    assert pool.acquired_connections == 0


@pytest.mark.asyncio
async def test_pool_connection_management(get_tcp_connections):
    """Tests connection cleanup when leaving a pool context.

    No dangling/unclosed connections must leave behind.
    """

    async def _get_pool_connection(pool: Pool):
        async with pool.connection():
            pass

    async with Connection() as conn:
        init_tcps = await get_tcp_connections(conn)

    async with Pool(minsize=1, maxsize=2) as pool:
        async with pool.connection():
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1
        assert pool.free_connections == 1
        assert pool.acquired_connections == 0

        async with pool.connection() as cn1:
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

            async with pool.connection() as cn2:
                assert pool.free_connections == 0
                assert pool.acquired_connections == 2

                # It is possible to acquire more than pool.maxsize property.
                # But the caller gets stuck while waiting for a free connection
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(_get_pool_connection(pool), timeout=1.0)

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
                assert pool.acquired_connections == 2

            assert pool.free_connections == 1
            assert pool.acquired_connections == 1

            async with pool.connection() as cn3:
                assert pool.free_connections == 0
                assert pool.acquired_connections == 2

                async with cn3.cursor() as cur:
                    await cur.execute("SELECT 84")
                    ret = await cur.fetchone()
                    assert ret == (84,)

            assert pool.free_connections == 1
            assert pool.acquired_connections == 1

        assert pool.free_connections == 2
        assert pool.acquired_connections == 0

    async with Connection() as conn:
        assert init_tcps == await get_tcp_connections(conn)


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

    async with Connection() as conn:
        init_tcps = await get_tcp_connections(conn)

    min_size, max_size = 10, 21
    selectees = list(range(min_size, max_size + 1))  # exceeding the maxsize
    answers = []
    async with Pool(minsize=min_size, maxsize=max_size) as pool:
        tasks = [
            asyncio.create_task(_test_pool_connection(pool=pool, selectee=selectee))
            for selectee in selectees
        ]
        answers = await asyncio.gather(*tasks)

    async with Connection() as conn:
        noc = await get_tcp_connections(conn)
        assert noc == init_tcps

    assert selectees == answers


@pytest.mark.asyncio
async def test_pool_broken_connection_handling():
    async def _get_answer(pool: Pool, *, raise_exc: bool = True):
        async with pool.connection() as conn_ctx:
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

            async with conn_ctx.cursor() as cur:
                if raise_exc:
                    raise AsynchPoolError("good bye")
                await cur.execute("SELECT 21 + 21;")
                ret = await cur.fetchone()
                assert ret == 42
                return ret

    min_size, max_size = 1, 1
    pool = Pool(minsize=min_size, maxsize=max_size)
    async with pool:
        async with pool.connection() as conn:
            await conn.ping()

            # he connection is invalidated
            await conn.close()
            with pytest.raises(ConnectionError):
                await conn.ping()

            # but does not influence the pool state
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

        # when leaving the connection context,
        # the pool should ensure its consistency
        assert pool.free_connections == 1
        assert pool.acquired_connections == 0

        async with pool.connection() as conn:
            await conn.ping()
            assert pool.free_connections == 0
            assert pool.acquired_connections == 1

        seq = list(range(10))
        tasks = [asyncio.create_task(_get_answer(pool=pool, raise_exc=bool(i % 2))) for i in seq]
        # no blockade and no inconsistency
        await asyncio.gather(*tasks, return_exceptions=True)

        assert pool.free_connections == 1
        assert pool.acquired_connections == 0

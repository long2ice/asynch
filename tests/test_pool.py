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
async def test_pool_connection_management(get_tcp_connections, assert_tcp_connections_settle):
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

    await assert_tcp_connections_settle(init_tcps)


@pytest.mark.asyncio
async def test_pool_concurrent_connection_management(
    get_tcp_connections, assert_tcp_connections_settle
):
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

    await assert_tcp_connections_settle(init_tcps)

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


@pytest.mark.asyncio
async def test_pool_discards_dead_free_connection():
    """A connection that died while idle must not be handed out.

    `_refresh` used to "reconnect" such a connection, but `connect()` returns
    early while the connection still looks opened, so the dead one was handed
    straight back to the caller.
    """
    async with Pool(minsize=1, maxsize=2) as pool:
        # Kill the pooled connection behind the pool's back.
        (dead,) = tuple(pool._free_connections)
        await dead._connection.disconnect()

        async with pool.connection() as conn:
            assert conn is not dead
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 42")
                assert await cursor.fetchone() == (42,)

        assert dead not in pool._free_connections
        assert dead not in pool._acquired_connections


@pytest.mark.asyncio
async def test_pool_reaps_idle_connections():
    """With `idle_timeout`, the pool shrinks back towards minsize.

    Without it the pool grows to its high-water mark and keeps every
    connection for the process lifetime.
    """
    async with Pool(minsize=1, maxsize=4, idle_timeout=0.2) as pool:
        barrier = asyncio.Barrier(5)

        async def hold():
            async with pool.connection():
                await barrier.wait()

        tasks = [asyncio.create_task(hold()) for _ in range(4)]
        await asyncio.sleep(0.1)
        assert _get_pool_size(pool) == 4
        await barrier.wait()
        await asyncio.gather(*tasks)
        assert _get_pool_size(pool) == 4

        await asyncio.sleep(0.3)
        # Any checkout runs the reaper on release.
        async with pool.connection():
            pass
        assert _get_pool_size(pool) == 1


@pytest.mark.asyncio
async def test_pool_keeps_connections_without_idle_timeout():
    """The default must stay as it was: no reaping."""
    async with Pool(minsize=1, maxsize=3) as pool:
        barrier = asyncio.Barrier(4)

        async def hold():
            async with pool.connection():
                await barrier.wait()

        tasks = [asyncio.create_task(hold()) for _ in range(3)]
        await asyncio.sleep(0.1)
        await barrier.wait()
        await asyncio.gather(*tasks)

        await asyncio.sleep(0.2)
        async with pool.connection():
            pass
        assert _get_pool_size(pool) == 3


@pytest.mark.asyncio
async def test_pool_liveness_grace_halves_pings():
    """A connection verified on release is not re-pinged on the next acquire."""
    calls = {"n": 0}
    original = Connection.is_live

    async def counting(self):
        calls["n"] += 1
        return await original(self)

    Connection.is_live = counting
    try:
        async with Pool(minsize=1, maxsize=1, liveness_grace=10) as pool:
            for _ in range(10):
                async with pool.connection():
                    pass
        with_grace = calls["n"]

        calls["n"] = 0
        async with Pool(minsize=1, maxsize=1, liveness_grace=0) as pool:
            for _ in range(10):
                async with pool.connection():
                    pass
        without_grace = calls["n"]
    finally:
        Connection.is_live = original

    assert without_grace >= 2 * with_grace - 2


def test_pool_rejects_invalid_idle_timeout():
    with pytest.raises(ValueError):
        Pool(idle_timeout=0)

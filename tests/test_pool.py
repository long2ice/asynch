import pytest

from asynch.connection import Connection
from asynch.pool import Pool, PoolError
from asynch.proto import constants


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
        f" object at 0x{id(pool):x}>"
    )
    assert repr(pool) == repstr

    minsize, maxsize = 2, 3
    pool = Pool(minsize=minsize, maxsize=maxsize)
    repstr = f"<Pool(minsize={minsize}, maxsize={maxsize}) object at 0x{id(pool):x}>"
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
async def test_connection_release(get_tcp_connections):
    """Tests connection cleanup when leaving pool context.

    Asserting that leaving pool context there are
    no dangling/unclosed connections left.
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

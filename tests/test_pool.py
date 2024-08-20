import pytest

from asynch.connection import Connection
from asynch.pool import Pool
from asynch.proto import constants


@pytest.mark.asyncio
async def test_pool_default(config):
    async with Pool(dsn=config.dsn) as pool:
        assert pool.minsize == constants.POOL_MIN_SIZE
        assert pool.maxsize == constants.POOL_MAX_SIZE
        assert pool.size == pool.get_connections()
        assert pool.freesize == pool.get_free_connections()
        assert pool.get_acquired_connections() == 0


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
        f"<Pool object at 0x{id(pool):x}: "
        f"minsize={constants.POOL_MIN_SIZE}, "
        f"maxsize={constants.POOL_MAX_SIZE}>"
    )
    assert repr(pool) == repstr

    minsize, maxsize = 2, 3
    pool = Pool(minsize=minsize, maxsize=maxsize)
    repstr = f"<Pool object at 0x{id(pool):x}: " f"minsize={minsize}, maxsize={maxsize}>"
    assert repr(pool) == repstr


@pytest.mark.asyncio
async def test_connection_release(get_tcp_connections):
    """Tests connection cleanup when leaving pool context.

    Asserting that leaving pool context there are
    no dangling/unclosed connections left.
    """

    conn = Connection()
    init_tcps: int = await get_tcp_connections(conn)

    min_size = 10
    for max_size in range(min_size, min_size + 5):
        async with Pool(minsize=min_size, maxsize=max_size) as pool:
            assert pool.get_free_connections() == min_size
            assert pool.get_acquired_connections() == 0
            pool_conn: Connection = ...
            for _ in range(1, max_size + 1):
                pool_conn = await pool.connection()
                async with pool_conn.cursor() as cursor:
                    await cursor.execute("SELECT -1")
                    ret = await cursor.fetchone()
                    assert ret == (-1,)
            assert pool.get_free_connections() == 0
            assert pool.get_acquired_connections() == max_size
        assert init_tcps == await get_tcp_connections(conn)

    assert init_tcps == await get_tcp_connections(conn)
    await conn.close()

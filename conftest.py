import asyncio
import pytest
import asynch
from asynch import connect
from asynch.cursors import DictCursor


@pytest.yield_fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    res = policy.new_event_loop()
    asyncio.set_event_loop(res)
    res._close = res.close
    res.close = lambda: None

    yield res

    res._close()


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    conn = await connect()
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute('create database if not exists test')
        await cursor.execute("""CREATE TABLE if not exists test.asynch
    (
        `id`       Int32,
        `decimal`  Decimal(10, 2),
        `date`     Date,
        `datetime` DateTime,
        `float`    Float32,
        `uuid`     UUID,
        `string`   String,
        `ipv4`     IPv4,
        `ipv6`     IPv6
    
    )
        ENGINE = MergeTree
            ORDER BY id""")


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    conn = await connect()
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("truncate table test.asynch")


@pytest.fixture(scope="function")
async def pool():
    pool = await asynch.create_pool()
    yield pool
    pool.close()
    await pool.wait_closed()

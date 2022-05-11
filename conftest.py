import asyncio
from asyncio.streams import StreamReader

import pytest

import asynch
from asynch import connect
from asynch.cursors import DictCursor
from asynch.proto import constants
from asynch.proto.context import Context
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


@pytest.fixture
def column_options():
    reader = BufferedReader(StreamReader(), constants.BUFFER_SIZE)
    writer = BufferedWriter()
    context = Context()
    context.client_settings = {
        "strings_as_bytes": False,
        "strings_encoding": constants.STRINGS_ENCODING,
    }
    column_options = {"reader": reader, "writer": writer, "context": context}
    return column_options


@pytest.fixture(scope="session")
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

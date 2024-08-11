import asyncio
import os
from typing import AsyncIterator

import pytest

from asynch.connection import Connection, connect
from asynch.cursors import DictCursor
from asynch.pool import create_pool
from asynch.proto import constants
from asynch.proto.context import Context
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


@pytest.fixture
async def column_options():
    reader = BufferedReader(asyncio.StreamReader(), constants.BUFFER_SIZE)
    writer = BufferedWriter()
    context = Context()
    context.client_settings = {
        "strings_as_bytes": False,
        "strings_encoding": constants.STRINGS_ENCODING,
    }
    column_options = {"reader": reader, "writer": writer, "context": context}
    yield column_options
    await writer.close()


CONNECTION_HOST = os.environ.get("CLICKHOUSE_HOST", default="127.0.0.1")
CONNECTION_PORT = os.environ.get("CLICKHOUSE_PORT", default="9000")
CONNECTION_USER = os.environ.get("CLICKHOUSE_USER", default="default")
CONNECTION_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", default="")
CONNECTION_DB = os.environ.get("CLICKHOUSE_DB", default="default")
CONNECTION_DSN = os.environ.get(
    "CLICKHOUSE_DSN",
    default=(
        f"clickhouse://{CONNECTION_USER}:{CONNECTION_PASSWORD}"
        f"@{CONNECTION_HOST}:{CONNECTION_PORT}"
        f"/{CONNECTION_DB}"
    )
)


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests():
    conn = await connect(dsn=CONNECTION_DSN)
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute('create database if not exists test')
        await cursor.execute('drop table if exists test.asynch')
        await cursor.execute(
            """
            CREATE TABLE if not exists test.asynch
            (
                `id`       Int32,
                `decimal`  Decimal(10, 2),
                `date`     Date,
                `datetime` DateTime,
                `float`    Float32,
                `uuid`     UUID,
                `string`   String,
                `ipv4`     IPv4,
                `ipv6`     IPv6,
                `bool`     Bool
            )
            ENGINE = MergeTree
            ORDER BY id
            """
        )
    yield
    await conn.close()


@pytest.fixture(scope="function", autouse=True)
async def truncate_table():
    conn = await connect(dsn=CONNECTION_DSN)
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("truncate table test.asynch")
    yield
    await conn.close()


@pytest.fixture(scope="function")
async def pool():
    pool = await create_pool(dsn=CONNECTION_DSN)
    yield pool
    pool.close()
    await pool.wait_closed()


@pytest.fixture(scope="function")
async def conn() -> AsyncIterator[Connection]:
    async with Connection(dsn=CONNECTION_DSN) as cn:
        yield cn


@pytest.fixture(scope="function")
async def conn_lz4() -> AsyncIterator[Connection]:
    async with Connection(dsn=CONNECTION_DSN, compression=True) as cn:
        yield cn


@pytest.fixture(scope="function")
async def conn_lz4hc() -> AsyncIterator[Connection]:
    async with Connection(dsn=CONNECTION_DSN, compression="lz4hc") as cn:
        yield cn


@pytest.fixture(scope="function")
async def conn_zstd() -> AsyncIterator[Connection]:
    async with Connection(dsn=CONNECTION_DSN, compression="zstd") as cn:
        yield cn

import asyncio
from dataclasses import dataclass
from os import environ
from typing import AsyncIterator

import pytest

from asynch.connection import Connection, connect
from asynch.cursors import DictCursor
from asynch.pool import Pool
from asynch.proto import constants
from asynch.proto.context import Context
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


CONNECTION_USER = environ.get("CLICKHOUSE_USER", default=constants.DEFAULT_USER)
CONNECTION_PASSWORD = environ.get("CLICKHOUSE_PASSWORD", default=constants.DEFAULT_PASSWORD)
CONNECTION_HOST = environ.get("CLICKHOUSE_HOST", default=constants.DEFAULT_HOST)
CONNECTION_PORT = environ.get("CLICKHOUSE_PORT", default=constants.DEFAULT_PORT)
CONNECTION_DB = environ.get("CLICKHOUSE_DB", default=constants.DEFAULT_DATABASE)
CONNECTION_DSN = environ.get(
    "CLICKHOUSE_DSN",
    default=(
        f"clickhouse://{CONNECTION_USER}:{CONNECTION_PASSWORD}"
        f"@{CONNECTION_HOST}:{CONNECTION_PORT}"
        f"/{CONNECTION_DB}"
    )
)

@dataclass
class DSN:
    dsn: str
    user: str
    password: str
    host: str
    port: int
    database: str


@pytest.fixture(scope="session")
def dsn() -> DSN:
    return DSN(
        dsn=CONNECTION_DSN,
        user=CONNECTION_USER,
        password=CONNECTION_PASSWORD,
        host=CONNECTION_HOST,
        port=CONNECTION_PORT,
        database=CONNECTION_DB,
    )


@pytest.fixture(scope="function")
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
async def pool() -> AsyncIterator[Pool]:
    pool = Pool(dsn=CONNECTION_DSN)
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


@pytest.fixture(scope="function")
async def get_tcp_connections():
    async def _get_tcp_connections(connection: Connection) -> int:
        stmt = "SELECT * FROM system.metrics WHERE metric = 'TCPConnection'"
        async with connection.cursor() as cur:
            await cur.execute(query=stmt)
            result = await cur.fetchall()
            return int(result[0][1])
    return _get_tcp_connections

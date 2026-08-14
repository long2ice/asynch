"""Create and drop the table used by the insert benchmark."""

from asynch import Connection
from benchmark import CONNECTION_DSN

TABLE_DDL = """
CREATE TABLE IF NOT EXISTS test.benchmark
(
    `id`      UInt32,
    `name`    String,
    `value`   Float64,
    `created` DateTime,
    `tags`    Array(String)
)
ENGINE = MergeTree
ORDER BY id
"""


async def get_connection() -> Connection:
    conn = Connection(dsn=CONNECTION_DSN)
    await conn.connect()
    return conn


async def setup_database() -> None:
    conn = await get_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
        await cursor.execute(TABLE_DDL)
        await cursor.execute("TRUNCATE TABLE test.benchmark")
    await conn.close()


async def cleanup_database() -> None:
    conn = await get_connection()
    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test.benchmark")
    await conn.close()

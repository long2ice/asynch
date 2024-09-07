from typing import Any

import pytest

from asynch.connection import Connection
from asynch.cursors import DictCursor
from asynch.proto import constants


@pytest.mark.asyncio
async def test_dict_cursor_repr(conn):
    repstr = "<DictCursor(connection={conn}, echo={echo}) object at 0x{cid:x};"

    echo = True
    async with conn.cursor(cursor=DictCursor, echo=echo) as cursor:
        repstr = repstr.format(conn=conn, echo=echo, cid=id(cursor))
        repstr = repstr + " status: {status}>"

        assert repr(cursor) == repstr.format(status="ready")

        await cursor.execute("SELECT 1")
        assert repr(cursor) == repstr.format(status="finished")

        ret = await cursor.fetchone()
        assert ret == {"1": 1}

    assert repr(cursor) == repstr.format(status="closed")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stmt", "answer"),
    [
        ("SELECT 42", [{"42": 42}]),
        ("SELECT -21 WHERE 1 != 1", []),
    ],
)
async def test_cursor_async_for(
    stmt: str,
    answer: list[dict[str, Any]],
    conn: Connection,
):
    result: list[dict[str, Any]] = []

    async with conn:
        async with conn.cursor(cursor=DictCursor) as cursor:
            cursor.set_stream_results(stream_results=True, max_row_buffer=1000)
            await cursor.execute(stmt)
            result = [row async for row in cursor]

    assert result == answer


@pytest.mark.asyncio
async def test_fetchone(conn: Connection):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchone()
        assert ret == (1,)

        await cursor.execute("SELECT * FROM system.tables")
        ret = await cursor.fetchall()
        assert isinstance(ret, list)


@pytest.mark.asyncio
async def test_fetchall(conn: Connection):
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [(1,)]


@pytest.mark.asyncio
async def test_dict_cursor(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [{"1": 1}]


@pytest.mark.asyncio
async def test_insert_dict(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.execute(
            """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES""",
            [
                {
                    "id": 1,
                    "decimal": 1,
                    "date": "2020-08-08",
                    "datetime": "2020-08-08 00:00:00",
                    "float": 1,
                    "uuid": "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
                    "string": "1",
                    "ipv4": "0.0.0.0",
                    "ipv6": "::",
                    "bool": True,
                }
            ],
        )
        assert rows == 1


@pytest.mark.asyncio
async def test_insert_tuple(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.execute(
            """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES""",
            [
                (
                    1,
                    1,
                    "2020-08-08",
                    "2020-08-08 00:00:00",
                    1,
                    "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
                    "1",
                    "0.0.0.0",
                    "::",
                    True,
                )
            ],
        )
        assert rows == 1


@pytest.mark.asyncio
async def test_executemany(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.executemany(
            """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES""",
            [
                (
                    1,
                    1,
                    "2020-08-08",
                    "2020-08-08 00:00:00",
                    1,
                    "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
                    "1",
                    "0.0.0.0",
                    "::",
                    True,
                ),
                (
                    1,
                    1,
                    "2020-08-08",
                    "2020-08-08 00:00:00",
                    1,
                    "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
                    "1",
                    "0.0.0.0",
                    "::",
                    True,
                ),
            ],
        )
        assert rows == 2


@pytest.mark.asyncio
async def test_table_ddl(conn: Connection):
    async with conn.cursor() as cursor:
        await cursor.execute("drop table if exists test.alter_table")
        create_table_sql = """
            CREATE TABLE test.alter_table
(
    `id` Int32
)
ENGINE = MergeTree
            ORDER BY id
            """
        await cursor.execute(create_table_sql)
        add_column_sql = """alter table test.alter_table add column c String"""
        await cursor.execute(add_column_sql)
        show_table_sql = """show create table test.alter_table"""
        await cursor.execute(show_table_sql)
        assert await cursor.fetchone() == (
            "CREATE TABLE test.alter_table\n(\n    `id` Int32,\n    `c` String\n)\nENGINE = MergeTree\nORDER BY id\nSETTINGS index_granularity = 8192",
        )
        await cursor.execute("drop table test.alter_table")


@pytest.mark.asyncio
async def test_insert_buffer_overflow(conn: Connection):
    old_buffer_size = constants.BUFFER_SIZE
    constants.BUFFER_SIZE = 2**6 + 1

    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE if exists test.test")

        create_table_sql = """CREATE TABLE test.test
(
    `i` Int32,
    `c1` String,
    `c2` String,
    `c3` String,
    `c4` String
) ENGINE = MergeTree ORDER BY i"""
        await cursor.execute(create_table_sql)
        await cursor.execute("INSERT INTO test.test VALUES", [(1, "t", "t", "t", "t")])
        await cursor.execute("DROP TABLE if exists test.test")

    constants.BUFFER_SIZE = old_buffer_size


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size, expected_size, with_select",
    [
        [0, 0, True],
        [10, 10, True],
        [100, 0, False],
    ],
    ids=[
        "empty",
        "10 elements",
        "without select",
    ],
)
async def test_cursror_iter(conn, size, expected_size, with_select):
    async with conn.cursor() as cursor:
        await cursor.execute("DROP TABLE IF EXISTS test.test")
        await cursor.execute("CREATE TABLE test.test (a UInt8) ENGINE=Memory")

        data = [(v,) for v in range(size)]
        await cursor.execute("INSERT INTO test.test (a) VALUES", data)
        if with_select:
            await cursor.execute("SELECT * FROM test.test")

        index = 0
        async for one in cursor:
            assert one == data[index]
            index += 1

        assert expected_size == index

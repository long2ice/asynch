import pytest

from asynch.connection import Connection
from asynch.cursors import DictCursor
from asynch.proto import constants

conn = Connection(host='192.168.15.103')


@pytest.mark.asyncio
async def test_fetchone():
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 1000)
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchone()
        assert ret == (1,)
        await cursor.fetchall()

        await cursor.execute("SELECT * FROM system.tables")
        ret = await cursor.fetchall()
        assert isinstance(ret, list)


@pytest.mark.asyncio
async def test_fetchall():
    async with conn.cursor() as cursor:
        cursor.set_stream_results(True, 1000)
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [(1,)]


@pytest.mark.asyncio
async def test_dict_cursor():
    async with conn.cursor(cursor=DictCursor) as cursor:
        cursor.set_stream_results(True, 1000)
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [{"1": 1}]


@pytest.mark.asyncio
async def test_insert_dict():
    async with conn.cursor(cursor=DictCursor) as cursor:
        cursor.set_stream_results(True, 1000)
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
async def test_insert_tuple():
    async with conn.cursor(cursor=DictCursor) as cursor:
        cursor.set_stream_results(True, 1000)
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
async def test_executemany():
    async with conn.cursor(cursor=DictCursor) as cursor:
        cursor.set_stream_results(True, 1000)
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


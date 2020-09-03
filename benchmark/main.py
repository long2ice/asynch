import asyncio
from datetime import date, datetime
from time import time
import uvloop
from clickhouse_driver import Client

from asynch import connect

insert_data = (
    1,
    1,
    date.today(),
    datetime.now(),
    1,
    "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
    "1",
    "0.0.0.0",
    "::",
)
sql = """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6) VALUES"""


async def init_table():
    conn = await connect()
    async with conn.cursor() as cursor:
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
        await cursor.execute("truncate table test.asynch")


def clickhouse_driver_insert():
    client = Client('localhost')
    start_time = time()
    data = []
    count = 0
    while time() - start_time < 10:
        data.append(insert_data)
        if len(data) == 10000:
            client.execute(sql, data)
            count += 10000
            print(count)
            data.clear()
    if data:
        client.execute(sql, data)
        count += len(data)
    print(count)
    # 720000


async def asynch_insert():
    conn = await connect()
    start_time = time()
    data = []
    count = 0
    async with conn.cursor() as cursor:
        while time() - start_time < 10:
            data.append(insert_data)
            if len(data) == 10000:
                await cursor.execute(sql, data)
                count += 10000
                print(count)
                data.clear()
        if data:
            await cursor.execute(sql, data)
            count += len(data)
    print(count)
    # 620000


if __name__ == '__main__':
    uvloop.install()
    asyncio.run(init_table())
    # clickhouse_driver_insert()
    asyncio.run(asynch_insert())

from time import time

from clickhouse_driver import Client

from asynch import connect

insert_data = (
    1,
    1,
    "2020-08-08",
    "2020-08-08 00:00:00",
    1,
    "59e182c4-545d-4f30-8b32-cefea2d0d5ba",
    "1",
    "0.0.0.0",
    "::",
)


async def create_table():
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


def clickhouse_driver_insert():
    client = Client('localhost')
    start_time = time()
    while time() - start_time < 10:
        pass


async def asynch_insert(conn):
    conn = await connect()
    async with conn.cursor() as cursor:
        pass


if __name__ == '__main__':
    pass

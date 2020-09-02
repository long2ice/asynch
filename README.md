# asynch

![pypi](https://img.shields.io/pypi/v/asynch.svg?style=flat)
![license](https://img.shields.io/github/license/long2ice/asynch)
![workflows](https://github.com/long2ice/asynch/workflows/pypi/badge.svg)
![workflows](https://github.com/long2ice/asynch/workflows/ci/badge.svg)

## Introduction

`asynch` is an asyncio ClickHouse Python Driver with native (TCP) interface support, which reuse most of [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) and comply with [PEP249](https://www.python.org/dev/peps/pep-0249/).

## Install

```shell
> pip install asynch
```

## Usage

### connect

```python
from asynch import connect

conn = connect(
    host: str = "127.0.0.1",
    port: int = 9000,
    database: str = "default",
    user: str = "default",
    password: str = "",
)
```

### create table

```python
async def create_table():
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute('create database if not exists test')
        await cursor.execute("""
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
                `ipv6`     IPv6

            )
            ENGINE = MergeTree
                ORDER BY id"""
        )
```

### fetchone

```python
async def fetchone():
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = cursor.fetchone()
        assert ret == (1,)
```

### fetchmany

```python
async def fetchall():
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = cursor.fetchall()
        assert ret == [(1,)]
```

### Use DictCursor

```python
async def dict_cursor():
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT 1")
        ret = cursor.fetchall()
        assert ret == [{"1": 1}]
```

### insert

```python
from asynch.cursors import DictCursor

async def insert_dict():
    async with conn.cursor(cursor=DictCursor) as cursor:
        ret = await cursor.execute(
            """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6) VALUES""",
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
                }
            ],
        )
        assert ret == 1
```

### insert with dict

```python
async def insert_tuple():
    async with conn.cursor(cursor=DictCursor) as cursor:
        ret = await cursor.execute(
            """INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6) VALUES""",
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
                )
            ],
        )
        assert ret == 1
```

### Connection Pool

```python
async def use_pool():
    pool = await asynch.create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            ret = cursor.fetchone()
            assert ret == (1,)
    pool.close()
    await pool.wait_closed()
```

## Notice

This project is under development and mostly forked from [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver). And this project not use in production, so PR and issue is great welcome.

## ThanksTo

- [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver), ClickHouse Python Driver with native interface support.

## License

This project is licensed under the [Apache-2.0](https://github.com/long2ice/asynch/blob/master/LICENSE) License.

# asynch

![pypi](https://img.shields.io/pypi/v/asynch.svg?style=flat)
![license](https://img.shields.io/github/license/long2ice/asynch)
![workflows](https://github.com/long2ice/asynch/workflows/pypi/badge.svg)
![workflows](https://github.com/long2ice/asynch/workflows/ci/badge.svg)

## Introduction

`asynch` is an asynchronous ClickHouse Python driver with native TCP interface support, which reuses most of [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) features and complies with [PEP249](https://www.python.org/dev/peps/pep-0249/).

## Installation

```shell
> pip install asynch
```

If you want to install [`clickhouse-cityhash`](https://pypi.org/project/clickhouse-cityhash/) to enable transport compression

```shell
> pip install asynch[compression]
```

## Usage

Basically, a connection to a ClickHouse server can be established in two ways:

1. with a DSN string, e.g., `clickhouse://[user:password]@host:port/database`;

    ```python
    from asynch import connect

    # connecting with a DSN string
    async def connect_database():
        conn = await connect(
            dsn = "clickhouse://ch_user:P@55w0rD:@127.0.0.1:9000/chdb",
        )
    ```

2. with separately given connection/DSN parameters: `user` (optional), `password` (optional), `host`, `port`, `database`.

    ```python
    from asynch import connect

    # connecting with DSN parameters
    async def connect_database():
        conn = await connect(
            user = "ch_user",
            password = "P@55w0rD",
            host = "127.0.0.1",
            port = 9000,
            database = "chdb",
        )
    ```

If a DSN string is given, it takes priority over any specified connection parameter.

Create a database and a table by executing SQL statements via an instance of the `Cursor` class (here its child `DictCursor` class) acquired from an instance of the `Connection` class.

```python
async def create_table(conn: Connection):
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
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
        ORDER BY id
        """
        )
```

Fetching one row from an executed SQL statement:

```python
async def fetchone(conn: Connection):
    # by default, an instance of the `Cursor` class
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchone()
        assert ret == (1,)
```

Fetching all the rows from an executed SQL statement:

```python
async def fetchall():
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [(1,)]
```

Using an instance of the `DictCursor` class to get results as a sequence of `dict`ionaries representing the rows of an executed SQL query:

```python
async def dict_cursor():
    async with conn.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [{"1": 1}]
```

Inserting data with `dict`s via a `DictCursor` instance:

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

Inserting data with `tuple`s:

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

Before the v0.2.4:

```python
async def use_pool():
    pool = await asynch.create_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            ret = await cursor.fetchone()
            assert ret == (1,)
    pool.close()
    await pool.wait_closed()
```

Since the v0.2.5:

```python
async def use_pool():
    # init a Pool and fill it with `minsize` opened connections
    async with Pool(minsize=1, maxsize=2) as pool:
        # acquire a connection from the pool
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                assert ret == (1,)
```

## ThanksTo

- [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver), ClickHouse Python Driver with native interface support.

## License

This project is licensed under the [Apache-2.0](https://github.com/long2ice/asynch/blob/master/LICENSE) License.

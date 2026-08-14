# asynch

![pypi](https://img.shields.io/pypi/v/asynch.svg?style=flat)
![license](https://img.shields.io/github/license/long2ice/asynch)
![workflows](https://github.com/long2ice/asynch/workflows/pypi/badge.svg)
![workflows](https://github.com/long2ice/asynch/workflows/ci/badge.svg)

## Introduction

`asynch` is an asynchronous ClickHouse Python driver with native TCP interface support, complying with [PEP 249](https://www.python.org/dev/peps/pep-0249/).

- **Fast**: the protocol hot path (streams, column codecs, connection, cursors) is compiled with Cython — it matches or beats [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver), the synchronous C-extension driver, while staying fully asynchronous (see [Performance](#performance))
- **asyncio-native**: `async`/`await` everywhere, with a built-in connection pool and streaming result sets
- **PEP 249 API**: `Connection`, `Cursor`/`DictCursor`, familiar `execute`/`fetch*` semantics
- **Typed**: ships `.pyi` stubs for the compiled modules (PEP 561)
- **Free-threading ready**: all compiled modules declare `freethreading_compatible`; importing asynch does not re-enable the GIL on free-threaded CPython

## Installation

```shell
> pip install asynch
```

Binary wheels are published for Linux (x86_64/arm64), Windows and macOS (Intel/ARM) on Python 3.9–3.14, including free-threaded builds — no compiler needed. On platforms without a wheel, the sdist compiles from source and requires a C toolchain.

If you want to install [`clickhouse-cityhash`](https://pypi.org/project/clickhouse-cityhash/) to enable transport compression

```shell
> pip install asynch[compression]
```

## Usage

Basically, a connection to a ClickHouse server can be established in two ways:

1. with a DSN string, e.g., `clickhouse://[user:password]@host:port/database`;

    ```python
    from asynch import Connection

    # connecting with a DSN string
    async def connect_database():
        async with Connection(
            dsn = "clickhouse://ch_user:P%4055w0rD@127.0.0.1:9000/chdb",
        ) as conn:
            pass
    ```

2. with separately given connection/DSN parameters: `user` (optional), `password` (optional), `host`, `port`, `database`.

    ```python
    from asynch import Connection

    # connecting with DSN parameters
    async def connect_database():
        async with Connection(
            user = "ch_user",
            password = "P@55w0rD",
            host = "127.0.0.1",
            port = 9000,
            database = "chdb",
        ) as conn:
            pass
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

Executing an SQL statement with parameters:

```python
async def execute(conn: Connection):
    async with conn.cursor() as cursor:
        await cursor.execute(
            """
            SELECT
                EXISTS(
                    SELECT 1
                    FROM table_a
                    WHERE profile_id = %(profile_id)s
                ) AS has_a,
                EXISTS(
                    SELECT 1
                    FROM table_b
                    WHERE profile_id = %(profile_id)s
                ) AS has_b
            """,
            {"profile_id": profile_id}
        )
        ret = await cursor.fetchone()
        assert ret == (True,)
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

### Streaming results

For result sets that should not be materialized in memory at once, enable
streaming and iterate the cursor: rows are fetched block by block from the
server.

```python
async def stream_rows(conn: Connection):
    async with conn.cursor() as cursor:
        cursor.set_stream_results(stream_results=True, max_row_buffer=65536)
        await cursor.execute("SELECT number FROM system.numbers LIMIT 1000000")
        async for row in cursor:
            process(row)
```

### Connection Pool

```python
from asynch import Pool

async def use_pool():
    # init a Pool and fill it with the `minsize` opened connections
    async with Pool(dsn="clickhouse://127.0.0.1:9000", minsize=1, maxsize=10) as pool:
        # acquire a connection from the pool
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                assert ret == (1,)
```

Or, you may open/close the pool manually:

```python
async def use_pool():
    pool = Pool(dsn="clickhouse://127.0.0.1:9000", minsize=1, maxsize=10)
    await pool.startup()

    # some logic

    await pool.shutdown()
```

## Performance

Since v0.4.0 the protocol hot path (streams, column codecs, connection,
cursors) is compiled with Cython, putting asynch on par with
[clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver) (the
synchronous C-extension driver) for most column types — while staying fully
asynchronous.

Sample results (Apple Silicon, ClickHouse 26.7, best of 3; run
`make benchmark` to reproduce on your own hardware):

| Scenario | asynch | clickhouse-driver | asynch vs driver |
| --- | ---: | ---: | ---: |
| Export 500k rows from a wide events table (8 mixed columns) | 438 ms | 728 ms | +66% |
| 100 concurrent queries (pool of 10) | 2103 queries/s | 1310 queries/s | +61% |
| Filtered slice (~1% of rows) | 4.2 ms | 4.0 ms | on par (server-bound) |
| GROUP BY aggregation over 500k rows | 5.6 ms | 5.6 ms | on par (server-bound) |
| Batch insert, 200k rows | 1.7 s | 1.6 s | on par (server-bound) |

Small queries and inserts are dominated by server work, where both drivers sit
at the wire limit; the asynchronous advantage shows once results get large or
queries run concurrently.

Column-type micro-benchmarks (500k-row SELECTs), for the decode paths behind
the numbers above:

| Case | asynch | clickhouse-driver | asynch vs driver |
| --- | ---: | ---: | ---: |
| Int64 | 24.6M rows/s | 24.7M rows/s | on par |
| Float64 | 23.2M rows/s | 20.1M rows/s | +15% |
| String | 21.7M rows/s | 15.2M rows/s | +43% |
| FixedString | 19.9M rows/s | 15.6M rows/s | +28% |
| Nullable(Int64) | 15.7M rows/s | 13.4M rows/s | +17% |
| Date | 17.8M rows/s | 15.3M rows/s | +16% |
| DateTime | 13.8M rows/s | 2.3M rows/s | +500% |
| DateTime64(3) | 9.8M rows/s | 2.2M rows/s | +345% |
| UUID | 3.7M rows/s | 2.4M rows/s | +56% |
| Decimal(10, 2) | 4.9M rows/s | 3.4M rows/s | +44% |
| LowCardinality(String) | 17.7M rows/s | 17.3M rows/s | +2% |
| Array(Int64) | 5.3M rows/s | 4.6M rows/s | +15% |
| Map(String, Int64) | 3.8M rows/s | 3.1M rows/s | +23% |
| Tuple(Int64, String) | 12.2M rows/s | 10.8M rows/s | +13% |

The benchmark suite lives in [benchmark/](./benchmark/):

```shell
# SELECT / INSERT / concurrency / pool scenarios, rich-table report
> make benchmark
# or a single scenario
> python -m benchmark.select
```

`BENCHMARK_ROWS` / `BENCHMARK_INSERT_ROWS` environment variables scale the
workload; `CLICKHOUSE_*` variables point it at a non-default server.

## Development

asynch is managed with [uv](https://docs.astral.sh/uv/); building it from
source needs a C compiler and Cython (wheels from PyPI do not).

```shell
# install all dependency groups and build the extensions in place
> make deps

# lint + typecheck + stubtest
> make check

# run the test suite (needs a local ClickHouse on port 9000)
> docker run -d -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server
> make test

# regenerate the .pyi stubs after changing a .pyx module
> make stubs
```

## ThanksTo

- [clickhouse-driver](https://github.com/mymarilyn/clickhouse-driver), ClickHouse Python Driver with native interface support.

## License

This project is licensed under the [Apache-2.0](https://github.com/long2ice/asynch/blob/master/LICENSE) License.

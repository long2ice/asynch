import pytest

from asynch.connection import Connection

# Compression requires the optional clickhouse-cityhash extra, which does not
# build on free-threaded CPython yet.
pytest.importorskip("clickhouse_cityhash")


@pytest.mark.asyncio
async def test_compress_lz4(config):
    async with Connection(dsn=config.dsn, compression=True) as conn_lz4:
        async with conn_lz4.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1


@pytest.mark.asyncio
async def test_compress_lz4hc(config):
    async with Connection(dsn=config.dsn, compression="lz4hc") as conn_lz4hc:
        async with conn_lz4hc.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1


@pytest.mark.asyncio
async def test_compress_zstd(config):
    async with Connection(dsn=config.dsn, compression="zstd") as conn_zstd:
        async with conn_zstd.cursor() as cursor:
            ret = await cursor.execute("SELECT 1")
            assert ret == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("compression", [True, "lz4hc", "zstd"])
async def test_compressed_insert_roundtrip(config, compression):
    """A compressed INSERT must not corrupt the stream.

    The block writer is a connection-level singleton that flushes once per
    block, so an insert already sends several blocks through it; a flush that
    kept its buffer re-sent every earlier byte and the server dropped the
    connection.
    """
    rows = [(i, f"name-{i}", [f"tag{i % 3}", "common"]) for i in range(20_000)]
    async with Connection(dsn=config.dsn, compression=compression) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("DROP TABLE IF EXISTS test.compressed_insert")
            await cursor.execute(
                "CREATE TABLE test.compressed_insert "
                "(id UInt32, name String, tags Array(String)) "
                "ENGINE = MergeTree ORDER BY id"
            )
            try:
                await cursor.execute(
                    "INSERT INTO test.compressed_insert (id, name, tags) VALUES", rows
                )
                await cursor.execute(
                    "SELECT count(), sum(id), sum(length(name)), sum(length(tags)) "
                    "FROM test.compressed_insert"
                )
                assert await cursor.fetchone() == (
                    len(rows),
                    sum(r[0] for r in rows),
                    sum(len(r[1]) for r in rows),
                    sum(len(r[2]) for r in rows),
                )
            finally:
                await cursor.execute("DROP TABLE IF EXISTS test.compressed_insert")

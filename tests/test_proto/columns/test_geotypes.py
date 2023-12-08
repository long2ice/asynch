from contextlib import asynccontextmanager

import pytest


@asynccontextmanager
async def create_table(cursor, spec):
    await cursor.execute("DROP TABLE IF EXISTS test.test")
    await cursor.execute(f"CREATE TABLE test.test ({spec}) engine=Memory")

    try:
        yield
    finally:
        await cursor.execute("DROP TABLE test.test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, data",
    [
        [
            "a Point",
            [
                ((1.5, 2),),
                ((3, 4),),
            ],
        ],
        [
            "a Ring",
            [
                ([(1.5, 2), (3, 4)],),
            ],
        ],
        [
            "a Polygon",
            [
                (
                    [
                        [(1.5, 2), (3, 4)],
                        [(5.5, 6), (7, 8)],
                    ],
                ),
            ],
        ],
        [
            "a MultiPolygon",
            [
                (
                    [
                        [
                            [(1.5, 2), (3, 4)],
                            [(5.5, 6), (7, 8)],
                        ],
                        [
                            [(2.5, 3), (4, 5)],
                            [(6.5, 7), (8, 9)],
                        ],
                    ],
                )
            ],
        ],
    ],
    ids=[
        "point",
        "ring",
        "polygon",
        "multipolygon",
    ],
)
async def test_get_types(conn, spec, data):
    async with conn.cursor() as cursor:
        await cursor.execute("set allow_experimental_geo_types = 1")
        async with create_table(cursor, spec):
            await cursor.execute("INSERT INTO test.test (a) VALUES", data)
            await cursor.execute("SELECT * FROM test.test")

            assert await cursor.fetchall() == data

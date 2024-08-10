from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, Optional

import pytest

from asynch.connection import Connection
from asynch.cursors import Cursor


@asynccontextmanager
async def create_table(cursor: Cursor, spec: str):
    await cursor.execute("DROP TABLE IF EXISTS test.test")
    await cursor.execute(f"CREATE TABLE test.test ({spec}) engine=Memory")

    try:
        yield
    finally:
        await cursor.execute("DROP TABLE test.test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("spec", "data", "cursor_settings"),
    [
        [
            "a Map(String, UInt64)",
            [
                ({},),
                ({"key1": 1},),
                ({"key1": 2, "key2": 20},),
                ({"key1": 3, "key2": 30, "key3": 50},),
            ],
            None,
        ],
        [
            "a Map(String, Nullable(UInt64))",
            [
                ({},),
                ({"key1": None},),
                ({"key1": 1},),
            ],
            None,
        ],
        [
            "a Map(LowCardinality(String), LowCardinality(UInt64))",
            [
                ({"key1": 1},),
                ({"key1": 1},),
                ({"key1": 1},),
            ],
            {"allow_suspicious_low_cardinality_types": True},
        ],
        [
            "a Map(String, Array(UInt64))",
            [
                ({"key1": []},),
                ({"key2": [1, 2, 3]},),
                ({"key3": [1, 1, 1, 1]},),
            ],
            None,
        ],
        [
            "a Map(String, Decimal(9, 2))",
            [
                ({"key1": Decimal("123.45")},),
                ({"key2": Decimal("234.56")},),
                ({"key3": Decimal("345.67")},),
            ],
            None,
        ],
    ],
    ids=[
        "simple",
        "nullable",
        "low_cardinally",
        "array",
        "decimal",
    ],
)
async def test_map_column(
    conn: Connection,
    spec: str,
    data: list[tuple[dict[str, Any], ...]],
    cursor_settings: Optional[dict[str, Any]],
):
    async with conn.cursor() as cursor:
        if cursor_settings:
            cursor.set_settings(settings=cursor_settings)
        async with create_table(cursor, spec):
            await cursor.execute("INSERT INTO test.test (a) VALUES", data)
            await cursor.execute("SELECT * FROM test.test")

            assert await cursor.fetchall() == data

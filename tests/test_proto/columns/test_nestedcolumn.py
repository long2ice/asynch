from contextlib import asynccontextmanager

import pytest

from asynch.proto.columns import nestedcolumn


@asynccontextmanager
async def create_table(cursor, spec):
    await cursor.execute("DROP TABLE IF EXISTS test.test")
    await cursor.execute("set flatten_nested = 0")
    await cursor.execute(f"CREATE TABLE test.test ({spec}) engine=Memory")

    try:
        yield
    finally:
        await cursor.execute("DROP TABLE test.test")
        await cursor.execute("set flatten_nested = 1")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "data, expected_dict",
    [
        [
            [([(0, "a"), (1, "b")],)],
            {
                "i": [([0, 1],)],
                "s": [(["a", "b"],)],
            },
        ],
        [
            [([(0, "a"), (1, "b")],), ([(3, "d"), (4, "e")],)],
            {
                "i": [([0, 1],), ([3, 4],)],
                "s": [(["a", "b"],), (["d", "e"],)],
            },
        ],
        [
            [
                {"n": [{"i": 0, "s": "a"}, {"i": 1, "s": "b"}]},
                {"n": [{"i": 3, "s": "d"}, {"i": 4, "s": "e"}]},
            ],
            {
                None: [([(0, "a"), (1, "b")],), ([(3, "d"), (4, "e")],)],
                "i": [([0, 1],), ([3, 4],)],
                "s": [(["a", "b"],), (["d", "e"],)],
            },
        ],
    ],
    ids=[
        "simple",
        "multiple_rows",
        "dict",
    ],
)
async def test_nested_column(conn, data, expected_dict):
    async with conn.cursor() as cursor:
        async with create_table(cursor, "n Nested(i Int32, s String)"):
            await cursor.execute("INSERT INTO test.test (n) VALUES", data)

            for select_str, expected in (
                ("*", expected_dict.get(None, data)),
                ("n.i", expected_dict["i"]),
                ("n.s", expected_dict["s"]),
            ):
                await cursor.execute(f"SELECT {select_str} FROM test.test")
                assert await cursor.fetchall() == expected


def test_get_nested_columns():
    assert nestedcolumn.get_nested_columns(
        "Nested(a Tuple(Array(Int8)),\n b Nullable(String))",
    ) == ["Tuple(Array(Int8))", "Nullable(String)"]


def test_get_columns_with_types():
    assert nestedcolumn.get_columns_with_types(
        "Nested(a Tuple(Array(Int8)),\n b Nullable(String))",
    ) == [("a", "Tuple(Array(Int8))"), ("b", "Nullable(String)")]

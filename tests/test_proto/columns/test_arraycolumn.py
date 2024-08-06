from contextlib import asynccontextmanager, nullcontext
from typing import ContextManager
from uuid import UUID

import pytest

from asynch import errors
from asynch.connection import Connection
from asynch.cursors import Cursor
from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.arraycolumn import ArrayColumn
from asynch.proto.columns.intcolumn import UInt8Column


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
    "spec, data, expectation",
    [
        [
            "a Array(Int32)",
            [([],)],
            nullcontext(),
        ],
        [
            "a Array(Int32)",
            [([100, 500],)],
            nullcontext(),
        ],
        [
            "a Array(Int32)",
            [([100, 500],), ([100, 500],)],
            nullcontext(),
        ],
        [
            "a Array(Array(Enum8('hello' = -1, 'world' = 2)))",
            [([["hello", "world"], ["hello"]],)],
            nullcontext(),
        ],
        [
            "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))",
            [
                (
                    [
                        [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
                        [[255, 255, 255], [255]],
                        [[255], [255], [255]],
                    ],
                    [
                        [[255, 170], [127, 127, 127, 127, 127], [170, 170, 170], [170]],
                        [[255, 255, 255], [255]],
                        [[255], [255], [255]],
                    ],
                )
            ],
            nullcontext(),
        ],
        [
            "a Array(Array(Array(Nullable(String))))",
            [
                ([[["str1_1", "str1_2", None], [None]], [["str1_3", "str1_4", None], [None]]],),
                ([[["str2_1", "str2_2", None], [None]]],),
                ([[["str3_1", "str3_2", None], [None]]],),
            ],
            nullcontext(),
        ],
        [
            "a Array(Array(Array(Int32))), b Array(Array(Array(Int32)))",
            [
                (
                    [],
                    [[]],
                ),
            ],
            nullcontext(),
        ],
        [
            "a Array(Int32)",
            [("test",)],
            pytest.raises(errors.TypeMismatchError),
        ],
        [
            "a Array(Int32)",
            [(["test"],)],
            pytest.raises(errors.TypeMismatchError),
        ],
        [
            "a Array(String)",
            [(["aaa", "bbb"],)],
            nullcontext(),
        ],
        ["a Array(Nullable(String))", [(["aaa", None, "bbb"],)], nullcontext()],
        [
            "a Array(UUID)",
            [
                (
                    [
                        UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
                        UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
                    ],
                )
            ],
            nullcontext(),
        ],
        [
            "a Array(Nullable(UUID))",
            [
                (
                    [
                        UUID("c0fcbba9-0752-44ed-a5d6-4dfb4342b89d"),
                        None,
                        UUID("2efcead4-ff55-4db5-bdb4-6b36a308d8e0"),
                    ],
                )
            ],
            nullcontext(),
        ],
        [
            "a Array(Tuple(Int32))",
            [([],)],
            nullcontext(),
        ],
    ],
    ids=[
        "empty",
        "simple",
        "column_as_nested_array",
        "nested_with_enum",
        "nested_of_nested",
        "multidimensional",
        "empty_nested",
        "type_mismatch_error",
        "type_mismatch_error2",
        "string_array",
        "uuid_array",
        "string_nullable_array",
        "uuid_nullable_array",
        "tuple_array",
    ],
)
async def test_array_column(
    spec: str,
    data: list[tuple[list, ...]],
    expectation: ContextManager,
    conn: Connection,
):
    async with conn.cursor() as cursor:
        async with create_table(cursor, spec):
            with expectation:
                await cursor.execute("INSERT INTO test.test (*) VALUES", data)
                await cursor.execute("SELECT * FROM test.test")

                assert await cursor.fetchall() == data


@pytest.fixture
def array_column(column_options):
    column = get_column_by_spec("Array(UInt8)", column_options)
    return column


def test_create_array_column(array_column):
    assert isinstance(array_column, ArrayColumn)
    assert isinstance(array_column.nested_column, UInt8Column)


@pytest.mark.asyncio
async def test_array_column_write_data_empty_items(array_column):
    await array_column.write_items([])

    assert len(array_column.writer.buffer) == 0


@pytest.mark.asyncio
async def test_array_column_write_data_items(array_column):
    await array_column.write_items([[1, 2, 3, 4]])

    assert len(array_column.writer.buffer) == 12

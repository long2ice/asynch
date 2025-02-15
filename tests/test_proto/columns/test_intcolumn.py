from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest

from asynch import errors
from asynch.proto.columns import get_column_by_spec


class Dialect:
    type_compiler = None


@dataclass
class Context:
    execution_options: dict
    dialect = Dialect


EmptyContext = Context(dict())


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
    "spec, data, expected, context, select_sql, expected_exc_dict",
    [
        [
            "a UInt8",
            [(300,)],
            [(44,)],
            Context(execution_options=dict(types_check=True)),
            "*",
            None,
        ],
        [
            "a Int8",
            [(-300,)],
            [(-44,)],
            Context(execution_options=dict(types_check=True)),
            "*",
            None,
        ],
        [
            "a UInt8",
            [(300,)],
            None,
            EmptyContext,
            None,
            dict(
                error=errors.TypeMismatchError,
                texts=["Column a", "types_check=True"],
            ),
        ],
        [
            "a UInt8",
            [(-1,)],
            None,
            Context(execution_options=dict(types_check=True)),
            None,
            dict(
                error=errors.TypeMismatchError,
                texts=['-1 for column "a"'],
            ),
        ],
        [
            "a Int8, b Int16, c Int32, d Int64, e UInt8, f UInt16, g UInt32, h UInt64",
            [(-10, -300, -123581321, -123581321345589144, 10, 300, 123581321, 123581321345589144)],
            [(-10, -300, -123581321, -123581321345589144, 10, 300, 123581321, 123581321345589144)],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a Int8, b Int16, c Int32, d Int64, e UInt8, f UInt16, g UInt32, h UInt64",
            [
                (
                    -128,
                    -32768,
                    -2147483648,
                    -9223372036854775808,
                    255,
                    65535,
                    4294967295,
                    18446744073709551615,
                ),
                (127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0),
            ],
            [
                (
                    -128,
                    -32768,
                    -2147483648,
                    -9223372036854775808,
                    255,
                    65535,
                    4294967295,
                    18446744073709551615,
                ),
                (127, 32767, 2147483647, 9223372036854775807, 0, 0, 0, 0),
            ],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a Nullable(Int32)",
            [(2,), (None,), (4,), (None,), (8,)],
            [(2,), (None,), (4,), (None,), (8,)],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a Int128",
            [
                (-170141183460469231731687303715884105728,),
                (-111111111111111111111111111111111111111,),
                (123,),
                (111111111111111111111111111111111111111,),
                (170141183460469231731687303715884105727,),
            ],
            [
                (-170141183460469231731687303715884105728,),
                (-111111111111111111111111111111111111111,),
                (123,),
                (111111111111111111111111111111111111111,),
                (170141183460469231731687303715884105727,),
            ],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a UInt128",
            [(0,), (123,), (340282366920938463463374607431768211455,)],
            [(0,), (123,), (340282366920938463463374607431768211455,)],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a Int256",
            [
                (
                    -57896044618658097711785492504343953926634992332820282019728792003956564819968,
                ),  # noqa: E501
                (
                    -11111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (123,),
                (
                    11111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (
                    57896044618658097711785492504343953926634992332820282019728792003956564819967,
                ),  # noqa: E501
            ],
            [
                (
                    -57896044618658097711785492504343953926634992332820282019728792003956564819968,
                ),  # noqa: E501
                (
                    -11111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (123,),
                (
                    11111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (
                    57896044618658097711785492504343953926634992332820282019728792003956564819967,
                ),  # noqa: E501
            ],
            EmptyContext,
            "*",
            None,
        ],
        [
            "a UInt256",
            [
                (0,),
                (123,),
                (
                    111111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (
                    115792089237316195423570985008687907853269984665640564039457584007913129639935,
                ),  # noqa: E501
            ],
            [
                (0,),
                (123,),
                (
                    111111111111111111111111111111111111111111111111111111111111111111111111111111,
                ),  # noqa: E501
                (
                    115792089237316195423570985008687907853269984665640564039457584007913129639935,
                ),  # noqa: E501
            ],
            EmptyContext,
            "*",
            None,
        ],
    ],
    ids=[
        "unsigned chop to type",
        "signed chop to type",
        "raise struct_error",
        "uint type_mismatch",
        "all sizes",
        "corner cases",
        "nullable",
        "int128",
        "uint128",
        "int256",
        "uint256",
    ],
)
async def test_int_column(conn, spec, data, expected, context, select_sql, expected_exc_dict):
    async with conn.cursor() as cursor:
        async with create_table(cursor, spec):
            try:
                await cursor.execute("INSERT INTO test.test (*) VALUES", data, context=context)
            except Exception as e:
                assert isinstance(e, expected_exc_dict["error"])
                if expected_exc_dict.get("texts"):
                    for error_text in expected_exc_dict["texts"]:
                        assert error_text in str(e)
                return

            await cursor.execute(f"SELECT {select_sql} FROM test.test")

            assert await cursor.fetchall() == expected or data


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, expected",
    [
        ["Int8", 1],
        ["UInt8", 1],
        ["Int16", 2],
        ["UInt16", 2],
        ["Int32", 4],
        ["UInt32", 4],
        ["Int64", 8],
        ["UInt64", 8],
        ["Int128", 16],
        ["UInt128", 16],
        ["Int256", 32],
        ["UInt256", 32],
    ],
    ids=[
        "int8",
        "uint8",
        "int16",
        "uint16",
        "int32",
        "uint32",
        "int64",
        "uint64",
        "int128",
        "uint128",
        "int256",
        "uint256",
    ],
)
async def test_int_column_write_data(column_options, spec, expected):
    column = get_column_by_spec(spec, column_options)
    await column.write_items([42 * (1 if spec.startswith("U") else -1)])

    assert len(column.writer.buffer) == expected

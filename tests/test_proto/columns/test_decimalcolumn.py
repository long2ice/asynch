from contextlib import asynccontextmanager
from decimal import Decimal

import pytest

from asynch import errors


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
    "spec, data, expected, select_sql, expected_exc",
    [
        [
            "a Decimal(9, 5)",
            [(Decimal("300.42"),), (300.42,), (-300,)],
            [(Decimal("300.42"),), (Decimal("300.42"),), (Decimal("-300"),)],
            "*",
            None,
        ],
        [
            "a Decimal32(2), b Decimal64(2), c Decimal128(2)",
            [
                (
                    Decimal("300.42"),
                    # 300.42 + (1 << 34)
                    Decimal("17179869484.42"),
                    # 300.42 + (1 << 100)
                    Decimal("1267650600228229401496703205676.42"),
                )
            ],
            None,
            "CAST(a AS String), CAST(b AS String), CAST(c AS String)",
            None,
        ],
        [
            "a Decimal32(2), b Decimal64(2), c Decimal128(2)",
            [
                (
                    Decimal("-300.42"),
                    # 300.42 + (1 << 34)
                    Decimal("-17179869484.42"),
                    # 300.42 + (1 << 100)
                    Decimal("-1267650600228229401496703205676.42"),
                )
            ],
            None,
            "CAST(a AS String), CAST(b AS String), CAST(c AS String)",
            None,
        ],
        [
            "a Decimal32(0), b Decimal64(0), c Decimal128(0)",
            [
                (
                    Decimal(10**9 - 1),
                    Decimal(10**18 - 1),
                    Decimal(10**38 - 1),
                ),
                (
                    Decimal(-(10**9) + 1),
                    Decimal(-(10**18) + 1),
                    Decimal(-(10**38) + 1),
                ),
            ],
            None,
            "*",
            None,
        ],
        [
            "a Nullable(Decimal32(3))",
            [(300.42,), (None,)],
            [(Decimal("300.42"),), (None,)],
            "*",
            None,
        ],
        [
            "a Decimal32(0)",
            [(2147483647,)],
            [(Decimal("2147483647"),)],
            "*",
            None,
        ],
        [
            "a Decimal32(0)",
            [(2147483647,)],
            None,
            "*",
            errors.TypeMismatchError,
        ],
        [
            "a Decimal(18, 2)",
            [(1.66,), (1.15,)],
            [(Decimal("1.66"),), (Decimal("1.15"),)],
            "*",
            None,
        ],
        [
            "a Decimal(8, 1)",
            [(1.6,), (1.0,), (12312.0,), (999999.6,)],
            [(Decimal("1.6"),), (Decimal("1.0"),), (Decimal("12312.0"),), (Decimal("999999.6"),)],
            "*",
            None,
        ],
        [
            "a Decimal(9, 4)",
            [(3.14159265358,), (2.7182,)],
            [(Decimal("3.1415"),), (Decimal("2.7182"),)],
            "*",
            None,
        ],
    ],
    ids=[
        "simple",
        "different_precisions",
        "different_precisions_negative",
        "max_precisions",
        "nullable",
        "no_scale",
        "type_mismatch",
        "preserve_precision",
        "precision_one_sign_after_point",
        "truncates_scale",
    ],
)
async def test_decimal_column(conn, spec, data, expected, select_sql, expected_exc):
    async with conn.cursor() as cursor:
        async with create_table(cursor, spec):
            try:
                await cursor.execute("INSERT INTO test.test (*) VALUES", data)
            except Exception as e:
                assert isinstance(e, expected_exc)
                return

            await cursor.execute(f"SELECT {select_sql} FROM test.test")

            assert await cursor.fetchall() == expected or data

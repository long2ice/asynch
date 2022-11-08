from contextlib import asynccontextmanager
from decimal import Decimal

import pytest


@asynccontextmanager
async def create_table(cursor, spec):
    await cursor.execute('DROP TABLE IF EXISTS test.test')
    await cursor.execute(f'CREATE TABLE test.test ({spec}) engine=Memory')

    try:
        yield
    finally:
        await cursor.execute('DROP TABLE test.test')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'spec, data',
    [
        [
            'a Map(String, UInt64)',
            [
                ({},),
                ({'key1': 1}, ),
                ({'key1': 2, 'key2': 20}, ),
                ({'key1': 3, 'key2': 30, 'key3': 50}, ),
            ],
        ],
        [
            'a Map(String, Nullable(UInt64))',
            [
                ({},),
                ({'key1': None},),
                ({'key1': 1}, ),
            ],
        ],
        [
            'a Map(LowCardinality(String), LowCardinality(UInt64))',
            [
                ({'key1': 1}, ),
                ({'key1': 1}, ),
                ({'key1': 1}, ),
            ],
        ],
        [
            'a Map(String, Array(UInt64))',
            [
                ({'key1': []}, ),
                ({'key2': [1, 2, 3]}, ),
                ({'key3': [1, 1, 1, 1]}, ),
            ],
        ],
        [
            'a Map(String, Decimal(9, 2))',
            [
                ({'key1': Decimal('123.45')}, ),
                ({'key2': Decimal('234.56')}, ),
                ({'key3': Decimal('345.67')}, )
            ],
        ]
    ],
    ids=[
        'simple',
        'nullable',
        'low_cardinally',
        'array',
        'decimal',
    ]
)
async def test_map_column(conn, spec, data):
    async with conn.cursor() as cursor:
        async with create_table(cursor, spec):
            await cursor.execute('INSERT INTO test.test (a) VALUES', data)
            await cursor.execute('SELECT * FROM test.test')

            assert await cursor.fetchall() == data

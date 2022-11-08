from typing import List
from dataclasses import dataclass
from functools import partial

import pytest

from asynch.cursors import DictCursor
from asynch.errors import ErrorCode, ServerException, TypeMismatchError


class Dialect:
    type_compiler = None


@dataclass
class Context:
    execution_options: dict
    dialect = Dialect


@dataclass
class ExternalTable:
    columns: List
    dialect_options: dict


stream_results_test_params = dict(
    argnames='method, data, expected, insert_sql, cursor_type',
    argvalues=[
        ['fetchone', None, (1,), None, None],
        ['fetchall', None, [(1,)], None, None],
        ['fetchall', None, [{'1': 1}], None, DictCursor],
        [
            None,
            [
                {
                    'id': 1,
                    'decimal': 1,
                    'date': '2020-08-08',
                    'datetime': '2020-08-08 00:00:00',
                    'float': 1,
                    'uuid': '59e182c4-545d-4f30-8b32-cefea2d0d5ba',
                    'string': '1',
                    'ipv4': '0.0.0.0',
                    'ipv6': '::',
                    'bool': True,
                }
            ],
            1,
            'INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES',
            DictCursor,
        ],
        [
            None,
            [
                (
                    1,
                    1,
                    '2020-08-08',
                    '2020-08-08 00:00:00',
                    1,
                    '59e182c4-545d-4f30-8b32-cefea2d0d5ba',
                    '1',
                    '0.0.0.0',
                    '::',
                    True,
                )
            ],
            1,
            'INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES',
            DictCursor,
        ],
        [
            None,
            [
                (
                    1,
                    1,
                    '2020-08-08',
                    '2020-08-08 00:00:00',
                    1,
                    '59e182c4-545d-4f30-8b32-cefea2d0d5ba',
                    '1',
                    '0.0.0.0',
                    '::',
                    True,
                ),
                (
                    1,
                    1,
                    '2020-08-08',
                    '2020-08-08 00:00:00',
                    1,
                    '59e182c4-545d-4f30-8b32-cefea2d0d5ba',
                    '1',
                    '0.0.0.0',
                    '::',
                    True,
                ),
            ],
            2,
            'INSERT INTO test.asynch(id,decimal,date,datetime,float,uuid,string,ipv4,ipv6,bool) VALUES',
            DictCursor,
        ]
    ],
    ids=[
        'fetchone',
        'fetchall',
        'dict_cursor',
        'insert_dict',
        'insert_tuple',
        'executemany',
    ]
)


async def _stream_results(cursor, execute, method, insert_sql, data):
    if not insert_sql:
        await execute('SELECT 1')
        return await getattr(cursor, method)()

    return await execute(insert_sql, data)


@pytest.mark.asyncio
@pytest.mark.parametrize(**stream_results_test_params)
async def test_set_stream_results(conn, method, data, expected, insert_sql, cursor_type):
    async with conn.cursor(cursor=cursor_type) as cursor:
        cursor.set_stream_results(True, 1000)
        result = await _stream_results(cursor, cursor.execute, method, insert_sql, data)

    assert result == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(**stream_results_test_params)
async def test_set_stream_results(conn, method, data, expected, insert_sql, cursor_type):
    context = Context(execution_options=dict(stream_results=True, max_block_size=1000))

    async with conn.cursor(cursor=cursor_type) as cursor:
        execute = partial(cursor.execute, context=context)
        result = await _stream_results(cursor, execute, method, insert_sql, data)

    assert result == expected


settings_test_params = dict(
    argnames='local_param_name, statement, expected, expected_exc_dict, settings',
    argvalues=[
        [
            'settings',
            'SELECT 1',
            dict(strings_encoding='utf-8'),
            None,
            dict(strings_encoding='utf-8'),
        ],
        [
            'rv',
            "SELECT name, value, changed FROM system.settings WHERE name = 'max_query_size'",
            [('max_query_size', '142', 1)],
            None,
            dict(max_query_size=142),
        ],
        [
            'rv',
            "SELECT name, value, changed FROM system.settings WHERE name = 'totals_auto_threshold'",
            [('totals_auto_threshold', '1.23', 1)],
            None,
            dict(totals_auto_threshold=1.23),
        ],
        [
            'rv',
            "SELECT name, value, changed FROM system.settings WHERE name = 'force_index_by_date'",
            [('force_index_by_date', '1', 1)],
            None,
            dict(force_index_by_date=1),
        ],
        [
            'rv',
            "SELECT name, value, changed FROM system.settings WHERE name = 'format_csv_delimiter'",
            [('format_csv_delimiter', 'd', 1)],
            None,
            dict(format_csv_delimiter='d'),
        ],
        [
            'rv',
            "SELECT name, value, changed FROM system.settings WHERE name = 'max_threads'",
            [('max_threads', '100500', 1)],
            None,
            dict(max_threads=100500),
        ],
        [
            'rv',
            'SELECT 1',
            [(1,)],
            None,
            dict(unknown_settings=12345),
        ],
        [
            'rv',
            'SELECT number FROM system.numbers LIMIT 10',
            None,
            dict(
                exc=ServerException,
                attr_path=['code'],
                expected_attr_values=[
                    ErrorCode.TOO_MANY_ROWS_OR_BYTES,
                    ErrorCode.TOO_MANY_ROWS,
                ]
            ),
            dict(max_result_rows=5),
        ],
        [
            'rv',
            'SELECT number FROM system.numbers LIMIT 10',
            [(0,), (1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,), (9,)],
            None,
            dict(max_result_rows=5, result_overflow_mode='break'),
        ],
    ],
    ids=[
        'immutable',
        'int',
        'float',
        'bool',
        'char',
        'max_threads',
        'unknown_setting',
        'max_result_rows',
        'result_overflow_mode_break',
    ]
)


async def _settings(cursor, execute, statement, exc_dict):
    try:
        await execute(statement)
    except Exception as e:
        if not isinstance(e, exc_dict['exc']):
            return False

        attr_value = None
        for path in exc_dict.get('attr_path', []):
            attr_value = getattr(e, path)
            e = attr_value

        if attr_value and attr_value not in exc_dict['expected_attr_values']:
            return False

    return await cursor.fetchall()


@pytest.mark.asyncio
@pytest.mark.parametrize(**settings_test_params)
async def test_set_settings(conn, local_param_name, statement, expected, expected_exc_dict, settings):
    async with conn.cursor() as cursor:
        cursor.set_settings(settings)
        rv = await _settings(cursor, cursor.execute, statement, expected_exc_dict)

    if expected:
        assert locals()[local_param_name] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(**settings_test_params)
async def test_settings_execution_options(conn, local_param_name, statement, expected, expected_exc_dict, settings):
    context = Context(execution_options=dict(settings=settings))

    async with conn.cursor() as cursor:
        execute = partial(cursor.execute, context=context)
        rv = await _settings(cursor, execute, statement, expected_exc_dict)

    if expected:
        assert locals()[local_param_name] == expected


types_check_test_params = dict(
    argnames='enabled, expected_exc_text',
    argvalues=[
        [False, 'Repeat query with types_check=True for detailed info'],
        [True, '-1 for column "x"'],
    ],
    ids=[
        'disabled',
        'enabled',
    ]
)


async def _types_check(cursor, execute):
    await cursor.execute('DROP TABLE IF EXISTS test.test_types_check')
    await cursor.execute('CREATE TABLE test.test_types_check (x UInt32) ENGINE=Memory')

    try:
        await execute('INSERT INTO test.test_types_check (x) VALUES', [{'x': -1}])
    except TypeMismatchError as e:
        await cursor.execute('DROP TABLE test.test_types_check')
        return e


@pytest.mark.asyncio
@pytest.mark.parametrize(**types_check_test_params)
async def test_set_types_check(conn, enabled, expected_exc_text):
    async with conn.cursor() as cursor:
        cursor.set_types_check(enabled)
        exc = await _types_check(cursor, cursor.execute)

    assert expected_exc_text in str(exc)


@pytest.mark.asyncio
@pytest.mark.parametrize(**types_check_test_params)
async def test_types_check_execution_options(conn, enabled, expected_exc_text):
    context = Context(execution_options=dict(types_check=enabled))

    async with conn.cursor() as cursor:
        execute = partial(cursor.execute, context=context)
        exc = await _types_check(cursor, execute)

    assert expected_exc_text in str(exc)


external_tables_test_params = dict(
    argnames='structure, data, expected, expected_exc',
    argvalues=[
        [
            [('x', 'Int32'), ('y', 'Array(Int32)')],
            [
                {'x': 100, 'y': [2, 4, 6, 8]},
                {'x': 500, 'y': [1, 3, 5, 7]},
            ],
            [(100, [2, 4, 6, 8]), (500, [1, 3, 5, 7])],
            None
        ],
        [
            [('x', 'Int32')],
            [],
            [],
            None
        ],
        [
            [],
            [],
            None,
            ValueError,
        ],
    ],
    ids=[
        'select',
        'send_empty_table',
        'send_empty_table_structure',
    ]
)


@pytest.mark.asyncio
@pytest.mark.parametrize(**external_tables_test_params)
async def test_set_external_tables(conn, structure, data, expected, expected_exc):
    async with conn.cursor() as cursor:
        cursor.set_external_table('test', structure, data)
        try:
            await cursor.execute('SELECT * from test')
        except Exception as e:
            assert isinstance(e, expected_exc)
            return

    assert await cursor.fetchall() == expected

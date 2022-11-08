from contextlib import asynccontextmanager
from ipaddress import IPv6Address, IPv4Address

import pytest

from asynch import errors
from asynch.connection import Connection
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.ipcolumn import IPv4Column, IPv6Column


@pytest.fixture()
def connection(conn: Connection) -> ProtoConnection:
    return conn._connection  # noqa


@asynccontextmanager
async def create_table(connection, spec):
    await connection.execute('DROP TABLE IF EXISTS test')
    await connection.execute(f'CREATE TABLE test ({spec}) engine=Memory')

    try:
        yield
    finally:
        await connection.execute('DROP TABLE test')


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'spec, data, expected, expected_exc, insert_options',
    [
        (
                'a IPv4',
                [(IPv4Address("10.0.0.1"),), (IPv4Address("192.168.253.42"),)],
                None,
                None,
                dict(),
        ),
        (
                'a IPv4',
                [(167772161,)],
                [(IPv4Address('10.0.0.1'),)],
                None,
                dict(),
        ),
        (
                'a IPv4',
                [("10.0.0.1",)],
                [(IPv4Address("10.0.0.1"),)],
                None,
                dict(),
        ),
        (
                'a IPv4',
                [('985.512.12.0',)],
                None,
                errors.CannotParseDomainError,
                dict(),
        ),
        (
                'a IPv4',
                [('985.512.12.0',)],
                None,
                errors.TypeMismatchError,
                dict(types_check=True),
        ),
        (
                'a Nullable(IPv4)',
                [(IPv4Address('10.10.10.10'),), (None,)],
                None,
                None,
                dict(),
        ),
        (
                'a IPv6',
                [
                        (IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),),
                        (IPv6Address('a22:cc64:cf47:1653:4976:3c0c:ff8d:417c'),),
                        (IPv6Address('12ff:0000:0000:0000:0000:0000:0000:0001'),)
                ],
                None,
                None,
                dict(),
        ),
        (
                'a IPv6',
                [('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae',)],
                [(IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),)],
                None,
                dict(),
        ),
        (
                'a IPv6',
                [(b"y\xf4\xe6\x98E\xde\xa5\x9b'e(\xe3\x8d:5\xae",)],
                [(IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),)],
                None,
                dict(),
        ),
        (
                'a IPv6',
                [('ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz',)],
                None,
                errors.CannotParseDomainError,
                dict(),
        ),
        (
                'a IPv6',
                [(1025.2147,)],
                None,
                errors.TypeMismatchError,
                dict(types_check=True),
        ),
        (
                'a IPv6',
                [('ghjk:e698:45de:a59b:2765:28e3:8d3a:zzzz',)],
                None,
                errors.TypeMismatchError,
                dict(types_check=True),
        ),
        (
                'a Nullable(IPv6)',
                [(IPv6Address('79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'),), (None,)],
                None,
                None,
                dict(),
        ),
    ],
    ids=[
        'ipv4_simple',
        'ipv4_from_int',
        'ipv4_from_str',
        'ipv4_cannot_parse_domain',
        'ipv4_type_mismatch',
        'ipv4_nullable',
        'ipv6_simple',
        'ipv6_from_str',
        'ipv6_from_bytes',
        'ipv6_cannot_parse_domain',
        'ipv6_type_mismatch',
        'ipv6_type_mismatch2',
        'ipv6_nullable',
    ]
)
async def test_ip(connection, spec, data, expected, expected_exc, insert_options):
    if expected is None and expected_exc is None:
        expected = data
    async with create_table(connection, spec):
        try:
            await connection.execute('INSERT INTO test (a) VALUES', data, **insert_options)
            inserted = await connection.execute('SELECT * FROM test')
        except Exception as e:
            assert isinstance(e, expected_exc)  # noqa
            return

        assert inserted == expected


@pytest.fixture
def ipv4_column(column_options):
    column = get_column_by_spec('IPv4', column_options)
    return column


@pytest.fixture
def ipv6_column(column_options):
    column = get_column_by_spec('IPv6', column_options)
    return column


def test_create_ip_column(ipv4_column, ipv6_column):
    assert isinstance(ipv4_column, IPv4Column)
    assert isinstance(ipv6_column, IPv6Column)


@pytest.mark.asyncio
async def test_ip_column_write_data(ipv4_column, ipv6_column):
    await ipv4_column.write_data('')
    assert len(ipv4_column.writer.buffer) == 0

    await ipv6_column.write_data('')
    assert len(ipv6_column.writer.buffer) == 0

    await ipv4_column.write_data(['10.0.0.1'])
    assert len(ipv4_column.writer.buffer) == 4

    await ipv6_column.write_data(['79f4:e698:45de:a59b:2765:28e3:8d3a:35ae'])
    assert len(ipv4_column.writer.buffer) == 20

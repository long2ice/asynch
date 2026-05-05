"""
SQLAlchemy DB-API interface compatibility tests.

These tests verify that asynch exposes every piece of the DB-API interface that
SQLAlchemy's dialect machinery relies on.  They operate at the raw asynch level
(no SQLAlchemy Core/ORM required) so they run without clickhouse-sqlalchemy.

SQLAlchemy reads or calls the following on a DB-API module / connection / cursor:
  - Module: apilevel, threadsafety, paramstyle
  - Module: exception classes (Error, DatabaseError, ...)
  - Connection: close(), commit(), rollback(), cursor()
  - Cursor: description (with proper type_code), rowcount, lastrowid
  - Cursor: execute(), fetchone(), fetchmany(), fetchall()
  - Cursor: setinputsizes(), setoutputsize()
"""

import datetime

import pytest

import asynch
from asynch.errors import NotSupportedError

SA_TABLE = "test.sa_compat"


class TestModuleInterfaceForSQLAlchemy:
    """SQLAlchemy reads these module attributes during dialect initialisation."""

    def test_apilevel_is_2_0(self):
        assert asynch.apilevel == "2.0"

    def test_threadsafety_is_valid(self):
        assert asynch.threadsafety in (0, 1, 2, 3)

    def test_paramstyle_is_valid(self):
        assert asynch.paramstyle in {"qmark", "numeric", "named", "format", "pyformat"}

    def test_error_class_exists(self):
        assert hasattr(asynch, "Error")
        assert issubclass(asynch.Error, Exception)

    def test_database_error_class_exists(self):
        assert hasattr(asynch, "DatabaseError")
        assert issubclass(asynch.DatabaseError, asynch.Error)

    def test_interface_error_class_exists(self):
        assert hasattr(asynch, "InterfaceError")
        assert issubclass(asynch.InterfaceError, asynch.Error)

    def test_operational_error_class_exists(self):
        assert hasattr(asynch, "OperationalError")

    def test_programming_error_class_exists(self):
        assert hasattr(asynch, "ProgrammingError")

    def test_not_supported_error_class_exists(self):
        assert hasattr(asynch, "NotSupportedError")


class TestConnectionInterfaceForSQLAlchemy:
    """SQLAlchemy calls these connection methods during its session lifecycle."""

    async def test_commit_does_not_raise(self, sa_conn):
        """SQLAlchemy always calls commit(); it must not raise."""
        await sa_conn.commit()

    async def test_rollback_acceptable(self, sa_conn):
        """SQLAlchemy calls rollback() on exception; must not raise unexpectedly."""
        try:
            await sa_conn.rollback()
        except NotSupportedError:
            pass  # valid for non-transactional DB

    async def test_cursor_returns_cursor(self, sa_conn):
        from asynch.cursors import Cursor

        cursor = sa_conn.cursor()
        assert isinstance(cursor, Cursor)

    async def test_close_works(self, config):
        conn = asynch.connect(dsn=config.dsn)
        await conn.connect()
        await conn.close()
        assert conn.closed


class TestCursorInterfaceForSQLAlchemy:
    """SQLAlchemy accesses these cursor attributes for result processing."""

    async def test_description_is_none_before_execute(self, sa_conn):
        cursor = sa_conn.cursor()
        assert cursor.description is None

    async def test_description_after_select_has_correct_format(self, sa_cursor):
        await sa_cursor.execute("SELECT toInt32(1) AS id, 'hello' AS name")
        desc = sa_cursor.description
        assert desc is not None
        assert len(desc) == 2
        for item in desc:
            name, type_code, *rest = item
            assert isinstance(name, str), "name must be str"
            assert type_code is not None, "type_code must not be None"
            assert not isinstance(type_code, str), (
                "type_code must be a PEP 249 type object, not a raw string"
            )

    async def test_type_code_maps_int_to_number(self, sa_cursor):
        await sa_cursor.execute("SELECT toInt32(42) AS n")
        assert sa_cursor.description[0][1] == asynch.NUMBER

    async def test_type_code_maps_string_to_string(self, sa_cursor):
        await sa_cursor.execute("SELECT 'hello' AS s")
        assert sa_cursor.description[0][1] == asynch.STRING

    async def test_type_code_maps_date_to_datetime(self, sa_cursor):
        await sa_cursor.execute("SELECT toDate('2024-01-01') AS d")
        assert sa_cursor.description[0][1] == asynch.DATETIME

    async def test_rowcount_exists(self, sa_cursor):
        await sa_cursor.execute("SELECT 1")
        assert sa_cursor.rowcount is not None
        assert isinstance(sa_cursor.rowcount, int)

    async def test_lastrowid_exists_and_accessible(self, sa_cursor):
        """SQLAlchemy reads lastrowid after INSERT for identity management."""
        await sa_cursor.execute(
            f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES",
            [(1, "test", 1.0, datetime.date(2024, 1, 1))],
        )
        _ = sa_cursor.lastrowid  # must not raise AttributeError

    async def test_lastrowid_is_none_for_clickhouse(self, sa_cursor):
        """ClickHouse has no auto-increment IDs; lastrowid must be None."""
        await sa_cursor.execute(
            f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES",
            [(2, "test", 2.0, datetime.date(2024, 1, 1))],
        )
        assert sa_cursor.lastrowid is None, (
            "ClickHouse does not provide row IDs; lastrowid must be None"
        )

    async def test_setinputsizes_no_raise(self, sa_cursor):
        await sa_cursor.execute("SELECT 1")
        sa_cursor.setinputsizes([])

    async def test_setoutputsize_no_raise(self, sa_conn):
        cursor = sa_conn.cursor()
        cursor.setoutputsize(1024)
        cursor.setoutputsize(1024, 0)


class TestCursorFetchForSQLAlchemy:
    """SQLAlchemy uses these fetch patterns during result processing."""

    async def test_fetchone_returns_tuple(self, sa_cursor):
        await sa_cursor.execute("SELECT 1 AS n")
        row = await sa_cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    async def test_fetchone_returns_none_exhausted(self, sa_cursor):
        await sa_cursor.execute("SELECT 1 AS n")
        await sa_cursor.fetchone()
        row = await sa_cursor.fetchone()
        assert row is None

    async def test_fetchmany_respects_size(self, sa_cursor, sa_conn):
        import datetime

        # insert 5 rows
        rows = [(i, f"r{i}", float(i), datetime.date(2024, 1, 1)) for i in range(1, 6)]
        async with sa_conn.cursor() as insert_cursor:
            await insert_cursor.executemany(
                f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES",
                rows,
            )
        await sa_cursor.execute(f"SELECT id FROM {SA_TABLE} ORDER BY id")
        batch = await sa_cursor.fetchmany(3)
        assert len(batch) == 3

    async def test_fetchall_returns_all(self, sa_cursor, sa_conn):
        rows = [(i, f"r{i}", float(i), datetime.date(2024, 1, 1)) for i in range(1, 4)]
        async with sa_conn.cursor() as insert_cursor:
            await insert_cursor.executemany(
                f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES",
                rows,
            )
        await sa_cursor.execute(f"SELECT id FROM {SA_TABLE} ORDER BY id")
        all_rows = await sa_cursor.fetchall()
        assert len(all_rows) == 3


class TestTransactionLifecycleForSQLAlchemy:
    """SQLAlchemy wraps operations in commit/rollback; both must behave gracefully."""

    async def test_commit_after_insert(self, sa_conn):
        async with sa_conn.cursor() as cursor:
            await cursor.execute(
                f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES",
                [(1, "x", 0.0, datetime.date(2024, 1, 1))],
            )
        await sa_conn.commit()  # must not raise

    async def test_rollback_after_select(self, sa_conn):
        async with sa_conn.cursor() as cursor:
            await cursor.execute(f"SELECT * FROM {SA_TABLE}")
        try:
            await sa_conn.rollback()
        except NotSupportedError:
            pass  # valid

    async def test_nested_commit_calls(self, sa_conn):
        await sa_conn.commit()
        await sa_conn.commit()  # must not raise on repeated calls

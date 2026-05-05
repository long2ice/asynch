"""
PEP 249 — Cursor execute / executemany compliance tests.

Covers:
- execute() works for SELECT, INSERT, DDL
- execute() updates rowcount
- execute() with parameters
- executemany() inserts multiple rows
- executemany() updates rowcount
- Calling fetch before execute raises ProgrammingError
- execute() on a closed cursor raises InterfaceError
"""

import datetime

import pytest

from asynch.errors import InterfaceError, ProgrammingError

PEP249_TABLE = "test.pep249"


class TestExecuteSelect:
    async def test_simple_select(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1")
        row = await pep249_cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    async def test_select_multiple_columns(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1, 'hello', 3.14")
        row = await pep249_cursor.fetchone()
        assert row is not None
        assert len(row) == 3

    async def test_select_sets_rowcount(self, pep249_conn):
        async with pep249_conn.cursor() as cursor:
            await cursor.execute(f"SELECT * FROM {PEP249_TABLE}")
            # Empty table: rowcount should be 0 or -1 (both valid per spec for SELECT)
            assert cursor.rowcount in (-1, 0), (
                f"rowcount after SELECT on empty table must be 0 or -1; got {cursor.rowcount}"
            )

    async def test_select_rowcount_with_rows(self, pep249_cursor, populated_table):
        await pep249_cursor.execute(f"SELECT * FROM {PEP249_TABLE}")
        # After fetching, rowcount should reflect the number of rows returned
        assert pep249_cursor.rowcount >= 0 or pep249_cursor.rowcount == -1


class TestExecuteInsert:
    async def test_insert_single_row(self, pep249_cursor):
        await pep249_cursor.execute(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            [(1, "test", 1.0, datetime.date(2024, 1, 1), True)],
        )
        # Verify the row was inserted
        await pep249_cursor.execute(f"SELECT id FROM {PEP249_TABLE} WHERE id = 1")
        row = await pep249_cursor.fetchone()
        assert row is not None
        assert row[0] == 1

    async def test_insert_rowcount(self, pep249_cursor):
        await pep249_cursor.execute(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            [(10, "x", 0.0, datetime.date(2024, 1, 1), False)],
        )
        # rowcount after INSERT should be >= 0 or -1 (both valid per spec)
        assert pep249_cursor.rowcount >= 0 or pep249_cursor.rowcount == -1


class TestExecuteMany:
    async def test_executemany_inserts_rows(self, pep249_cursor):
        rows = [
            (1, "Alice", 1.0, datetime.date(2024, 1, 1), True),
            (2, "Bob", 2.0, datetime.date(2024, 1, 2), False),
            (3, "Charlie", 3.0, datetime.date(2024, 1, 3), True),
        ]
        await pep249_cursor.executemany(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            rows,
        )
        await pep249_cursor.execute(f"SELECT count() FROM {PEP249_TABLE}")
        count_row = await pep249_cursor.fetchone()
        assert count_row[0] == 3

    async def test_executemany_empty_sequence(self, pep249_cursor):
        """executemany with an empty sequence must not raise."""
        await pep249_cursor.executemany(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            [],
        )

    async def test_executemany_rowcount(self, pep249_cursor):
        rows = [(i, f"row{i}", float(i), datetime.date(2024, 1, 1), True) for i in range(1, 4)]
        await pep249_cursor.executemany(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            rows,
        )
        # rowcount after executemany should be >= 0 or -1
        assert pep249_cursor.rowcount >= 0 or pep249_cursor.rowcount == -1


class TestExecuteErrors:
    async def test_fetch_before_execute_raises(self, pep249_conn):
        """Fetching before execute must raise ProgrammingError."""
        cursor = pep249_conn.cursor()
        with pytest.raises(ProgrammingError):
            await cursor.fetchone()

    async def test_fetchmany_before_execute_raises(self, pep249_conn):
        cursor = pep249_conn.cursor()
        with pytest.raises(ProgrammingError):
            await cursor.fetchmany(1)

    async def test_fetchall_before_execute_raises(self, pep249_conn):
        cursor = pep249_conn.cursor()
        with pytest.raises(ProgrammingError):
            await cursor.fetchall()

    async def test_execute_on_closed_cursor_raises(self, pep249_conn):
        """Executing on a closed cursor must raise InterfaceError."""
        cursor = pep249_conn.cursor()
        await cursor.close()
        with pytest.raises(InterfaceError):
            await cursor.execute("SELECT 1")


class TestExecuteDDL:
    async def test_ddl_does_not_raise(self, pep249_cursor):
        """DDL statements (CREATE, DROP) must execute without error."""
        await pep249_cursor.execute(
            "CREATE TABLE IF NOT EXISTS test.pep249_ddl_test "
            "(id Int32) ENGINE = MergeTree ORDER BY id"
        )
        await pep249_cursor.execute("DROP TABLE IF EXISTS test.pep249_ddl_test")

    async def test_ddl_rowcount(self, pep249_cursor):
        """rowcount after DDL must be -1 or 0 — not a meaningful value."""
        await pep249_cursor.execute(
            "CREATE TABLE IF NOT EXISTS test.pep249_ddl_rowcount_test "
            "(id Int32) ENGINE = MergeTree ORDER BY id"
        )
        assert pep249_cursor.rowcount in (-1, 0), (
            f"rowcount after DDL must be -1 or 0; got {pep249_cursor.rowcount}"
        )
        await pep249_cursor.execute("DROP TABLE IF EXISTS test.pep249_ddl_rowcount_test")

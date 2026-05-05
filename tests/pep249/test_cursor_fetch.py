"""
PEP 249 — Cursor fetch method compliance tests.

Covers:
- fetchone() returns a single row as a sequence (tuple)
- fetchone() returns None when result is exhausted
- fetchmany() returns a sequence of rows
- fetchmany() uses arraysize when no size is given
- fetchmany() returns empty list when exhausted
- fetchall() returns all remaining rows
- fetchall() returns empty list when exhausted
- Rows are sequences (indexable, iterable)
"""

import datetime

import pytest

PEP249_TABLE = "test.pep249"


@pytest.fixture
async def three_rows(pep249_cursor, populated_table):
    """Execute a SELECT that returns the three pre-inserted rows, yield cursor."""
    await pep249_cursor.execute(f"SELECT id, name, value FROM {PEP249_TABLE} ORDER BY id")
    yield pep249_cursor


class TestFetchone:
    async def test_returns_tuple_or_sequence(self, three_rows):
        row = await three_rows.fetchone()
        assert row is not None
        assert hasattr(row, "__getitem__"), "fetchone() must return an indexable sequence"

    async def test_first_row_correct(self, three_rows):
        row = await three_rows.fetchone()
        assert row[0] == 1  # id

    async def test_successive_calls_advance_position(self, three_rows):
        row1 = await three_rows.fetchone()
        row2 = await three_rows.fetchone()
        assert row1[0] != row2[0], "successive fetchone() calls must advance position"

    async def test_returns_none_when_exhausted(self, three_rows):
        await three_rows.fetchone()
        await three_rows.fetchone()
        await three_rows.fetchone()
        result = await three_rows.fetchone()
        assert result is None, "fetchone() must return None when result set is exhausted"

    async def test_empty_result(self, pep249_cursor):
        await pep249_cursor.execute(f"SELECT * FROM {PEP249_TABLE} WHERE id = -999")
        result = await pep249_cursor.fetchone()
        assert result is None, "fetchone() must return None for empty result set"


class TestFetchmany:
    async def test_returns_list(self, three_rows):
        rows = await three_rows.fetchmany(2)
        assert isinstance(rows, list), "fetchmany() must return a list"

    async def test_returns_up_to_size_rows(self, three_rows):
        rows = await three_rows.fetchmany(2)
        assert len(rows) == 2

    async def test_returns_remaining_if_fewer_than_size(self, three_rows):
        rows = await three_rows.fetchmany(10)
        assert len(rows) == 3, "fetchmany() must return all remaining rows if fewer than size"

    async def test_returns_empty_when_exhausted(self, three_rows):
        await three_rows.fetchmany(3)  # consume all
        rows = await three_rows.fetchmany(1)
        assert rows == [], "fetchmany() must return [] when result is exhausted"

    async def test_uses_arraysize_default(self, pep249_cursor, populated_table):
        pep249_cursor.arraysize = 2
        await pep249_cursor.execute(f"SELECT id FROM {PEP249_TABLE} ORDER BY id")
        rows = await pep249_cursor.fetchmany()  # no explicit size
        assert len(rows) == 2, "fetchmany() with no size arg must use arraysize"

    async def test_size_zero_returns_empty(self, three_rows):
        rows = await three_rows.fetchmany(0)
        assert rows == [], "fetchmany(0) must return []"

    async def test_each_row_is_sequence(self, three_rows):
        rows = await three_rows.fetchmany(2)
        for row in rows:
            assert hasattr(row, "__getitem__"), "each row in fetchmany() must be indexable"


class TestFetchall:
    async def test_returns_list(self, three_rows):
        rows = await three_rows.fetchall()
        assert isinstance(rows, list), "fetchall() must return a list"

    async def test_returns_all_rows(self, three_rows):
        rows = await three_rows.fetchall()
        assert len(rows) == 3

    async def test_row_content(self, three_rows):
        rows = await three_rows.fetchall()
        ids = [row[0] for row in rows]
        assert sorted(ids) == [1, 2, 3]

    async def test_returns_empty_when_exhausted(self, three_rows):
        await three_rows.fetchall()
        rows = await three_rows.fetchall()
        assert rows == [], "fetchall() must return [] when result is already exhausted"

    async def test_empty_table(self, pep249_cursor):
        await pep249_cursor.execute(f"SELECT * FROM {PEP249_TABLE}")
        rows = await pep249_cursor.fetchall()
        assert rows == [], "fetchall() must return [] for empty result set"


class TestFetchInterleaving:
    """fetchone and fetchmany can be interleaved."""

    async def test_fetchone_then_fetchall(self, three_rows):
        first = await three_rows.fetchone()
        rest = await three_rows.fetchall()
        assert first is not None
        assert len(rest) == 2

    async def test_fetchmany_then_fetchone(self, three_rows):
        two = await three_rows.fetchmany(2)
        last = await three_rows.fetchone()
        none = await three_rows.fetchone()
        assert len(two) == 2
        assert last is not None
        assert none is None


class TestRowFormat:
    """Rows returned must be sequences of Python-native values."""

    async def test_row_values_are_python_types(self, pep249_cursor, populated_table):
        await pep249_cursor.execute(
            f"SELECT id, name, value, created, flag FROM {PEP249_TABLE} WHERE id = 1"
        )
        row = await pep249_cursor.fetchone()
        assert row is not None
        id_val, name_val, value_val, created_val, flag_val = row
        assert isinstance(id_val, int)
        assert isinstance(name_val, str)
        assert isinstance(value_val, float)
        assert isinstance(created_val, datetime.date)
        assert isinstance(flag_val, bool)

    async def test_null_maps_to_none(self, pep249_cursor, populated_table):
        """SQL NULL must map to Python None per PEP 249."""
        await pep249_cursor.execute(f"SELECT name FROM {PEP249_TABLE} WHERE id = 3")
        row = await pep249_cursor.fetchone()
        assert row is not None
        assert row[0] is None, "SQL NULL must map to Python None"

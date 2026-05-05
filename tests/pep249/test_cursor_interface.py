"""
PEP 249 — Cursor interface compliance tests (attributes and method presence).

Covers:
- description attribute exists and is readable
- rowcount attribute exists and is readable
- arraysize attribute exists as a public read/write property
- connection attribute returns the parent connection
- callproc method exists (may raise NotSupportedError)
- close method exists
- execute method exists
- executemany method exists
- fetchone method exists
- fetchmany method exists
- fetchall method exists
- nextset method exists (may return None)
- setinputsizes method exists
- setoutputsize (singular) method exists
- lastrowid attribute exists (SQLAlchemy optional extension)
"""

import inspect

import pytest

from asynch.connection import Connection
from asynch.cursors import Cursor
from asynch.errors import InterfaceError, NotSupportedError


class TestCursorAttributePresence:
    """Every required PEP 249 attribute must exist on the Cursor class."""

    REQUIRED_ATTRS = [
        "description",
        "rowcount",
        "arraysize",
        "connection",
    ]

    @pytest.mark.parametrize("attr", REQUIRED_ATTRS)
    def test_attribute_exists(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert hasattr(cursor, attr), f"Cursor must have attribute '{attr}'"

    def test_lastrowid_exists(self, pep249_conn):
        """lastrowid is a SQLAlchemy-required optional extension."""
        cursor = pep249_conn.cursor()
        assert hasattr(cursor, "lastrowid"), (
            "Cursor must have 'lastrowid' attribute (required by SQLAlchemy)"
        )


class TestCursorMethodPresence:
    """Every required PEP 249 method must exist and be callable on the Cursor class."""

    REQUIRED_METHODS = [
        "close",
        "execute",
        "executemany",
        "fetchone",
        "fetchmany",
        "fetchall",
        "nextset",
        "setinputsizes",
        "setoutputsize",
        "callproc",
    ]

    @pytest.mark.parametrize("method", REQUIRED_METHODS)
    def test_method_exists(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert hasattr(cursor, method), f"Cursor must have method '{method}'"

    @pytest.mark.parametrize("method", REQUIRED_METHODS)
    def test_method_is_callable(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert callable(getattr(cursor, method)), f"cursor.{method} must be callable"

    def test_setoutputsize_singular(self, pep249_conn):
        """PEP 249 specifies 'setoutputsize' (singular), not 'setoutputsizes'."""
        cursor = pep249_conn.cursor()
        assert hasattr(cursor, "setoutputsize"), (
            "Cursor must have 'setoutputsize' (singular) per PEP 249"
        )


class TestCursorArraysize:
    """arraysize must be a public read/write attribute defaulting to 1."""

    def test_arraysize_default_is_one(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert cursor.arraysize == 1, "arraysize must default to 1 per PEP 249"

    def test_arraysize_is_writable(self, pep249_conn):
        cursor = pep249_conn.cursor()
        cursor.arraysize = 10
        assert cursor.arraysize == 10

    def test_arraysize_set_to_various_values(self, pep249_conn):
        cursor = pep249_conn.cursor()
        for val in (1, 5, 100, 1000):
            cursor.arraysize = val
            assert cursor.arraysize == val


class TestCursorConnection:
    """cursor.connection must return the parent Connection object."""

    def test_connection_is_parent(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert cursor.connection is pep249_conn

    async def test_connection_accessible_after_execute(self, pep249_conn):
        async with pep249_conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert cursor.connection is pep249_conn


class TestCursorRowcount:
    """rowcount must be -1 before any execute."""

    def test_rowcount_initial_value(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert cursor.rowcount == -1, "rowcount must be -1 before any execute() call"


class TestCursorDescriptionInitial:
    """description must be None before any execute."""

    def test_description_none_before_execute(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert cursor.description is None, "description must be None before any execute() call"


class TestCursorCallproc:
    """callproc must exist; ClickHouse may raise NotSupportedError."""

    async def test_callproc_raises_not_supported_or_works(self, pep249_cursor):
        try:
            await pep249_cursor.callproc("nonexistent_proc")
        except NotSupportedError:
            pass  # valid — ClickHouse has no stored procedures
        except Exception as exc:
            pytest.fail(f"callproc raised an unexpected exception: {type(exc).__name__}: {exc}")


class TestCursorNextset:
    """nextset must exist; for ClickHouse (single result set) it returns None."""

    async def test_nextset_returns_none_or_does_not_raise(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1")
        try:
            result = await pep249_cursor.nextset()
            # If supported: returns True (has more sets) or None (no more)
            assert result is None or result is True, (
                f"nextset() must return None or True; got {result!r}"
            )
        except NotSupportedError:
            pass  # valid per PEP 249


class TestCursorSetinputsizes:
    """setinputsizes is a no-op per spec; must not raise."""

    async def test_setinputsizes_no_raise(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1")
        pep249_cursor.setinputsizes([10, 20])  # must not raise

    def test_setinputsizes_empty(self, pep249_conn):
        cursor = pep249_conn.cursor()
        cursor.setinputsizes([])  # must not raise


class TestCursorSetoutputsize:
    """setoutputsize (singular) is a no-op per spec; must not raise."""

    def test_setoutputsize_single_arg(self, pep249_conn):
        cursor = pep249_conn.cursor()
        cursor.setoutputsize(1024)  # must not raise

    def test_setoutputsize_with_column(self, pep249_conn):
        cursor = pep249_conn.cursor()
        cursor.setoutputsize(1024, 0)  # must not raise


class TestCursorClose:
    """close() must prevent further use of the cursor."""

    async def test_close_prevents_execute(self, pep249_conn):
        cursor = pep249_conn.cursor()
        await cursor.close()
        with pytest.raises((InterfaceError, Exception)):
            await cursor.execute("SELECT 1")

    async def test_close_is_idempotent(self, pep249_conn):
        cursor = pep249_conn.cursor()
        await cursor.close()
        await cursor.close()  # second close must not raise


class TestCursorLastrowid:
    """lastrowid is a SQLAlchemy-required optional extension."""

    async def test_lastrowid_exists(self, pep249_cursor):
        assert hasattr(pep249_cursor, "lastrowid"), (
            "cursor.lastrowid must exist for SQLAlchemy compatibility"
        )

    async def test_lastrowid_after_select(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1")
        # For ClickHouse, lastrowid is None (no auto-generated IDs)
        # It must not raise AttributeError
        _ = pep249_cursor.lastrowid

    async def test_lastrowid_before_execute(self, pep249_conn):
        cursor = pep249_conn.cursor()
        _ = cursor.lastrowid  # must not raise

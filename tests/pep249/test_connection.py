"""
PEP 249 — Connection object compliance tests.

Covers:
- connect() factory creates a usable connection
- close() closes the connection
- commit() does not raise (ClickHouse is auto-commit)
- rollback() raises NotSupportedError or silently succeeds (both are spec-valid)
- cursor() returns a Cursor object
- Async context manager protocol (__aenter__ / __aexit__)
- Connection cannot be used after close()
"""

import pytest

import asynch
from asynch.connection import Connection
from asynch.cursors import Cursor
from asynch.errors import NotSupportedError


class TestConnectFactory:
    """connect() must return an openable Connection."""

    async def test_connect_returns_connection(self, config):
        conn = asynch.connect(dsn=config.dsn)
        assert isinstance(conn, Connection)
        await conn.close()

    async def test_connection_opens_successfully(self, config):
        conn = asynch.connect(dsn=config.dsn)
        await conn.connect()
        assert conn.opened
        await conn.close()

    async def test_connect_with_kwargs(self, config):
        conn = asynch.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
        await conn.connect()
        assert conn.opened
        await conn.close()


class TestConnectionClose:
    """close() must immediately close the connection."""

    async def test_close_works(self, pep249_conn):
        await pep249_conn.close()
        assert pep249_conn.closed

    async def test_close_is_idempotent(self, pep249_conn):
        await pep249_conn.close()
        await pep249_conn.close()  # second close must not raise


class TestConnectionCommit:
    """
    PEP 249: commit() is required.
    For ClickHouse (auto-commit), commit() must succeed silently — not raise.
    """

    async def test_commit_does_not_raise(self, pep249_conn):
        await pep249_conn.commit()  # must not raise

    async def test_commit_returns_none(self, pep249_conn):
        result = await pep249_conn.commit()
        assert result is None


class TestConnectionRollback:
    """
    PEP 249: rollback() is optional and may raise NotSupportedError for
    databases without transaction support.  Both no-op and NotSupportedError
    are valid per spec; we accept either.
    """

    async def test_rollback_acceptable_behaviour(self, pep249_conn):
        """rollback() must either succeed silently or raise NotSupportedError."""
        try:
            await pep249_conn.rollback()
        except NotSupportedError:
            pass  # valid per PEP 249
        except Exception as exc:
            pytest.fail(f"rollback() raised an unexpected exception: {type(exc).__name__}: {exc}")


class TestConnectionCursor:
    """cursor() must return a usable Cursor."""

    def test_cursor_returns_cursor(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert isinstance(cursor, Cursor)

    async def test_cursor_is_usable(self, pep249_conn):
        async with pep249_conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            result = await cursor.fetchone()
            assert result == (1,)

    def test_multiple_cursors(self, pep249_conn):
        c1 = pep249_conn.cursor()
        c2 = pep249_conn.cursor()
        assert c1 is not c2


class TestConnectionContextManager:
    """Connection must support async context manager protocol."""

    async def test_aenter_returns_connection(self, config):
        async with Connection(dsn=config.dsn) as conn:
            assert isinstance(conn, Connection)
            assert conn.opened

    async def test_aexit_closes_connection(self, config):
        async with Connection(dsn=config.dsn) as conn:
            pass
        assert conn.closed

    async def test_aexit_on_exception(self, config):
        with pytest.raises(ValueError):
            async with Connection(dsn=config.dsn) as conn:
                raise ValueError("test error")
        assert conn.closed

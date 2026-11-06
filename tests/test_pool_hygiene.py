"""A connection left mid-query must never be handed out again.

The protocol layer only clears `is_query_executing` when the server's
end-of-stream packet is read, so a query that dies half-read (a cancelled
request, a dropped client) leaves the flag set. The pool does not check it on
its own, so it has to discard such a connection rather than return it to the
free queue, where every later checkout would fail on it.

These tests use a fake protocol connection and need no server.
"""

import asyncio

import pytest

from asynch.connection import Connection
from asynch.pool import Pool


class FakeStreamWriter:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class FakeBufferedWriter:
    def __init__(self) -> None:
        self.writer = FakeStreamWriter()


class FakeProtoConnection:
    """Mimics the protocol connection state the wrapper and pool rely on."""

    def __init__(self, is_query_executing: bool = False) -> None:
        self.is_query_executing = is_query_executing
        self.connected = True
        self.writer = FakeBufferedWriter()
        self.stream_writer = self.writer.writer
        self.disconnect_calls = 0
        self.ping_calls = 0

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.reset_state()

    async def ping(self) -> bool:
        self.ping_calls += 1
        return True

    def reset_state(self) -> None:
        self.writer = None
        self.connected = None
        self.is_query_executing = False


def _make_connection(is_query_executing: bool = False) -> Connection:
    conn = Connection(host="localhost")
    conn._connection = FakeProtoConnection(is_query_executing)
    conn._opened = True
    return conn


def _make_pool(*connections: Connection, **kwargs) -> Pool:
    pool = Pool(minsize=0, maxsize=max(len(connections), 1), **kwargs)
    pool._free_connections.extend(connections)
    return pool


def test_invalidate_closes_transport_resets_state_and_marks_closed():
    conn = _make_connection(is_query_executing=True)
    proto = conn._connection
    stream_writer = proto.stream_writer

    conn.invalidate()

    assert stream_writer.close_calls == 1
    assert proto.is_query_executing is False
    assert conn.closed
    assert not conn.opened


def test_invalidate_is_idempotent():
    conn = _make_connection(is_query_executing=True)

    conn.invalidate()
    conn.invalidate()

    assert conn.closed
    assert not conn.opened


def test_invalidate_survives_transport_and_reset_failures():
    conn = _make_connection(is_query_executing=True)
    proto = conn._connection

    def failing_close() -> None:
        raise OSError("socket already gone")

    def failing_reset() -> None:
        raise RuntimeError("reset failed")

    proto.stream_writer.close = failing_close
    proto.reset_state = failing_reset

    conn.invalidate()

    assert conn.closed
    assert not conn.opened


async def test_close_timeout_still_invalidates_connection():
    conn = _make_connection(is_query_executing=True)

    async def hanging_disconnect() -> None:
        await asyncio.Event().wait()

    conn._connection.disconnect = hanging_disconnect

    with pytest.raises(TimeoutError):
        await conn.close(timeout=0.01)

    assert conn._connection.is_query_executing is False
    assert conn.closed


async def test_cancelling_close_still_invalidates_connection():
    conn = _make_connection(is_query_executing=True)
    started = asyncio.Event()

    async def hanging_disconnect() -> None:
        started.set()
        await asyncio.Event().wait()

    conn._connection.disconnect = hanging_disconnect
    task = asyncio.create_task(conn.close())
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert conn._connection.is_query_executing is False
    assert conn.closed


async def test_is_live_does_not_ping_a_mid_query_connection():
    conn = _make_connection(is_query_executing=True)

    # is_live() no longer checks mid-query state; use _is_mid_query() instead
    assert conn._is_mid_query() is True
    # but is_live() still works if the connection is open
    assert await conn.is_live() is True
    # and doesn't ping unnecessarily during the liveness check
    assert conn._connection.ping_calls == 1  # only the initial ping in is_live()


async def test_pool_keeps_clean_connection():
    conn = _make_connection()
    pool = _make_pool(conn)

    async with pool.connection() as acquired:
        assert acquired is conn

    assert pool.free_connections == 1
    assert pool.acquired_connections == 0
    assert not conn.closed
    assert conn._connection.disconnect_calls == 0


async def test_pool_discards_connection_released_mid_query():
    conn = _make_connection()
    pool = _make_pool(conn, invalidate_conn_for_mid_query_releases=True)

    with pytest.raises(RuntimeError, match="boom"):
        async with pool.connection() as acquired:
            # the query dies half-read: the flag is still set on the way out
            acquired._connection.is_query_executing = True
            raise RuntimeError("boom")

    assert pool.free_connections == 0
    assert pool.acquired_connections == 0
    assert conn.closed
    assert conn._connection.is_query_executing is False
    # only the checkout ping: the dirty connection is not pinged on release
    assert conn._connection.ping_calls == 1


async def test_pool_discards_connection_when_request_is_cancelled_mid_query():
    conn = _make_connection()
    pool = _make_pool(conn, invalidate_conn_for_mid_query_releases=True)

    async def slow_request() -> None:
        async with pool.connection() as acquired:
            acquired._connection.is_query_executing = True
            await asyncio.sleep(10)

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(slow_request(), timeout=0.05)

    assert pool.free_connections == 0
    assert pool.acquired_connections == 0
    assert conn.closed


async def test_mid_query_connection_is_invalidated_before_waiting_for_the_pool_lock():
    """A second cancellation while the release waits for the lock must not
    leave the dirty connection reusable."""
    conn = _make_connection()
    pool = _make_pool(conn, invalidate_conn_for_mid_query_releases=True)
    inside = asyncio.Event()

    async def request() -> None:
        async with pool.connection() as acquired:
            acquired._connection.is_query_executing = True
            inside.set()
            await asyncio.sleep(10)

    task = asyncio.create_task(request())
    await inside.wait()

    async with pool._lock:
        task.cancel()
        await asyncio.sleep(0.01)
        # the release is parked on the lock, but the connection is already dead
        assert not task.done()
        assert conn.closed
        assert conn._connection.is_query_executing is False

    with pytest.raises(asyncio.CancelledError):
        await task

    assert pool.free_connections == 0
    assert pool.acquired_connections == 0


async def test_pool_hands_out_a_clean_connection_after_discarding_a_dirty_one():
    dirty = _make_connection()
    clean = _make_connection()
    pool = _make_pool(dirty, clean)

    with pytest.raises(RuntimeError):
        async with pool.connection() as acquired:
            assert acquired is dirty
            acquired._connection.is_query_executing = True
            raise RuntimeError("boom")

    async with pool.connection() as acquired:
        assert acquired is clean


async def test_discard_bounds_a_hanging_disconnect():
    conn = _make_connection()
    pool = _make_pool(conn, close_timeout=0.01)

    async def hanging_disconnect() -> None:
        await asyncio.Event().wait()

    conn._connection.disconnect = hanging_disconnect

    await asyncio.wait_for(pool._discard_connection(conn), timeout=1)

    assert conn.closed


def test_pool_rejects_invalid_close_timeout():
    with pytest.raises(ValueError, match=r"close_timeout is expected to be greater than zero"):
        Pool(close_timeout=0)

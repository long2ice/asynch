import asyncio
import logging
from collections import deque
from collections.abc import Coroutine
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
from warnings import warn

from asynch.connection import Connection, connect
from asynch.errors import ClickHouseException
from asynch.proto import constants
from asynch.proto.models.enums import PoolStatus

logger = logging.getLogger(__name__)


class AsynchPoolError(ClickHouseException):
    pass


class _ContextManager(Coroutine):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __iter__(self):
        return self._coro.__await__()

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ("_coro", "_conn", "_pool")

    def __init__(self, coro, pool):
        super().__init__(coro)
        self._coro = coro
        self._conn = None
        self._pool = pool

    async def __aenter__(self):
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class Pool(asyncio.AbstractServer):
    def __init__(
        self,
        minsize: int = constants.POOL_MIN_SIZE,
        maxsize: int = constants.POOL_MAX_SIZE,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs,
    ):
        if maxsize < 1:
            raise ValueError("maxsize is expected to be greater than zero")
        if minsize < 0:
            raise ValueError("minsize is expected to be greater or equal to zero")
        if minsize > maxsize:
            raise ValueError("minsize is greater than maxsize")
        self._maxsize = maxsize
        self._minsize = minsize
        self._connection_kwargs = kwargs
        self._terminated: set[Connection] = set()
        self._used: set[Connection] = set()
        self._cond = asyncio.Condition()
        self._closing = False
        self._lock = asyncio.Lock()
        self._acquired_connections: deque[Connection] = deque(maxlen=maxsize)
        self._free_connections: deque[Connection] = deque(maxlen=maxsize)
        self._opened: Optional[bool] = None
        self._closed: Optional[bool] = None
        warn(
            (
                "The loop parameter must be removed when Python3.10 "
                "will be the minimum version (in a year) or earlier."
            ),
            DeprecationWarning,
        )
        self._loop = (
            loop if isinstance(loop, asyncio.AbstractEventLoop) else asyncio.get_running_loop()
        )

    async def __aenter__(self) -> "Pool":
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        status = self.status
        return (
            f"<{cls_name}(minsize={self._minsize}, maxsize={self._maxsize})"
            f" object at 0x{id(self):x}; status: {status}>"
        )

    @property
    def status(self) -> str:
        """Return the status of the pool.

        If pool.opened is None and pool.closed is None,
        then the pool is in the "created" state.
        It was neither opened nor closed.

        When executing `async with pool: ...`,
        the `pool.opened` is True and `pool.closed` is None.
        When leaving the context, the `pool.closed` is True
        and the `pool.opened` is False.

        :raise AsynchPoolError: an unresolved pool state.
        :return: the Pool object status
        :rtype: str (PoolStatus StrEnum)
        """

        if self._opened is None and self._closed is None:
            return PoolStatus.created
        if self._opened:
            return PoolStatus.opened
        if self._closed:
            return PoolStatus.closed
        raise AsynchPoolError(f"{self} is in an unknown state")

    @property
    def closed(self) -> Optional[bool]:
        """Returns the pool close status.

        If the return value is None,
        the pool was only created,
        but neither activated or closed.

        :returns: the connection close status
        :rtype: None | bool
        """

        return self._closed

    @property
    def connections(self) -> int:
        """Returns the number of connections in the pool.

        This number represents the current size of the pool
        which is the sum of the acquired and free connections.

        :return: the number of connections in the pool
        :rtype: int
        """

        return self.acquired_connections + self.free_connections

    @property
    def acquired_connections(self) -> int:
        """Returns the number of connections acquired from the pool.

        A connection is acquired when `pool.connection()` is invoked.

        :return: the number of connections requested the pool
        :rtype: int
        """

        return len(self._acquired_connections)

    @property
    def free_connections(self) -> int:
        """Returns the number of free connections in the pool.

        :return: the number of connections in the pool
        :rtype: int
        """

        return len(self._free_connections)

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def freesize(self) -> int:
        warn(
            "Consider using `pool.free_connections` property instead of this `freesize`",
            DeprecationWarning,
        )
        return len(self._free_connections)

    @property
    def size(self) -> int:
        warn(
            "Consider using `pool.connections` property instead of this `size`",
            DeprecationWarning,
        )
        return self.freesize + len(self._used)

    @property
    def cond(self) -> asyncio.Condition:
        warn(
            "Scheduled for removal in the version 0.2.6 or later",
            DeprecationWarning,
        )
        return self._cond

    async def _create_connection(self) -> None:
        pool_size, maxsize = self.connections, self.maxsize
        if pool_size == maxsize:
            raise AsynchPoolError(f"{self} is already full")
        if pool_size > maxsize:
            raise RuntimeError(f"{self} is overburden")

        conn = await connect(**self._connection_kwargs)
        self._free_connections.append(conn)

    async def _acquire_connection(self) -> Connection:
        if not self._free_connections:
            raise AsynchPoolError(f"no free connection in the {self}")

        conn = self._free_connections.popleft()
        self._acquired_connections.append(conn)
        return conn

    async def _release_connection(self, conn: Connection) -> None:
        if conn not in self._acquired_connections:
            raise AsynchPoolError(f"the connection {conn} does not belong to the {self}")

        self._acquired_connections.remove(conn)
        self._free_connections.append(conn)

    async def _fill_with_connections(self, n: Optional[int] = None) -> None:
        to_create = n if n else self.minsize
        if to_create < 0:
            raise ValueError(f"cannot create negative connections ({to_create}) for {self}")
        tasks: list[asyncio.Task] = [
            asyncio.create_task(self._create_connection()) for _ in range(to_create)
        ]
        await asyncio.wait(fs=tasks)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        """Get a connection from the pool.

        :raises AsynchPoolError: if a connection cannot be acquired
        :raises AsynchPoolError: if a connection cannot be released

        :return: a free connection from the pool
        :rtype: Connection
        """

        async with self._lock:
            if not self._free_connections:
                to_create = min(self.minsize, self.maxsize - self.connections)
                await self._fill_with_connections(to_create)
            conn = await self._acquire_connection()
        try:
            yield conn
        finally:
            async with self._lock:
                await self._release_connection(conn)

    async def startup(self) -> "Pool":
        """Initialise the pool.

        When entering the context, the pool get filled with connections
        up to the pool `minsize` value.
        """

        if self._opened:
            return self
        async with self._lock:
            await self._fill_with_connections(n=self.minsize)
            self._opened = True
            if self._closed:
                self._closed = False
        return self

    async def shutdown(self) -> None:
        """Close the pool.

        This method closes consequently free connections first.
        Then it does the same for the acquired/active connections.
        Then the pool is marked closed.
        """

        async with self._lock:
            while self._free_connections:
                conn = self._free_connections.popleft()
                await conn.close()
            while self._acquired_connections:
                conn = self._acquired_connections.popleft()
                await conn.close()
            self._opened = False
            self._closed = True

    async def release(self, connection: Connection):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """

        warn(
            (
                "Consider using the `async with` approach for resource cleanup. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        fut = self._loop.create_future()
        fut.set_result(None)

        if connection in self._terminated:
            self._terminated.remove(connection)
            return fut
        self._used.remove(connection)
        if connection.connected:
            if self._closing:
                await connection.close()
            else:
                connection._connection.reset_state()
                self._free_connections.append(connection)
        fut = self._loop.create_task(self._wakeup())
        return fut

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def _wait(self):
        return len(self._terminated) > 0

    async def _check_conn(self, conn: Connection) -> bool:
        try:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(e)
            return False

    def acquire(self):
        warn(
            (
                "Consider using the `async with` approach for resource management. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        return _PoolAcquireContextManager(self._acquire(), self)

    async def _acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self.initialize()  # Restore minsize

                if not self._free_connections and self.size < self.maxsize:
                    await self.init_one_connection()

                if self._free_connections:
                    conn = self._free_connections.popleft()
                    if await self._check_conn(conn):
                        self._used.add(conn)
                        return conn
                    else:
                        continue
                else:
                    await self._cond.wait()

    async def initialize(self):
        warn(
            (
                "Consider using the `async with` approach for resource management. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        while self.freesize < self.minsize and self.size <= self.maxsize:
            await self.init_one_connection()

    async def init_one_connection(self):
        warn(
            (
                "Consider using the `async with` approach for resource management. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )
        conn = await connect(**self._connection_kwargs)
        self._free_connections.append(conn)
        self._cond.notify()

    async def clear(self) -> None:
        """Closes all free connections in the pool."""

        warn(
            (
                "Consider using the `async with` approach for resource cleanup. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        async with self._cond:
            while self._free_connections:
                conn = self._free_connections.popleft()
                await conn.close()
            self._cond.notify()

    async def wait_closed(self):
        """Waits for closing all connections in the pool."""

        warn(
            (
                "Consider using the `async with` approach for resource cleanup. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called " "after .close()")

        while self._free_connections:
            conn = self._free_connections.popleft()
            await conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()
        self._closed = True

    def close(self):
        """Close the pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """

        warn(
            (
                "Consider using the `async with` approach for resource cleanup. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        if self._closed:
            return
        self._closing = True

    async def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        warn(
            (
                "Consider using the `async with` approach for resource cleanup. "
                "Should be removed in the version 0.2.6 or later."
            ),
            DeprecationWarning,
        )

        self.close()
        for conn in self._used:
            await conn.close()
            self._terminated.add(conn)
        self._used.clear()


async def create_async_pool(
    minsize: int = constants.POOL_MIN_SIZE,
    maxsize: int = constants.POOL_MAX_SIZE,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
) -> Pool:
    """Returns an initiated connection pool.

    Equivalent to:
    1. pool = Pool(...)
    2. await pool.startup()
    3. return pool

    The behaviour above is analogous to the `asynch.connection.connect(...)` function.
    Do not forget to call `await pool.shutdown()` to cleanup pool resources.

    :param minsize int: the minimum number of connections in the pool
    :param maxsize int: the maximum number of connections in the pool
    :param loop Optional[asyncio.AbstractEventLoop]: an event loop (asyncio.get_running_loop() by default)
    :param kwargs dict: connection settings

    :return: a connection pool object
    :rtype: Pool
    """

    warn(
        (
            "This function is the future prototype of the `create_pool` function. "
            "Should be removed in the version 0.2.6 or later."
        ),
        DeprecationWarning,
    )

    pool = Pool(
        minsize=minsize,
        maxsize=maxsize,
        loop=loop,
        **kwargs,
    )
    await pool.startup()
    return pool


def create_pool(
    minsize: int = constants.POOL_MIN_SIZE,
    maxsize: int = constants.POOL_MAX_SIZE,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
):
    warn(
        (
            "This function should become asynchronous and behave like `create_async_pool`. "
            "The changes should take place in the version 0.2.6 or later."
        ),
        UserWarning,
    )
    coro = _create_pool(minsize=minsize, maxsize=maxsize, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(
    minsize: int = constants.POOL_MIN_SIZE,
    maxsize: int = constants.POOL_MAX_SIZE,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
):
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = Pool(minsize, maxsize, loop, **kwargs)
    if minsize > 0:
        async with pool.cond:
            await pool.initialize()
    return pool

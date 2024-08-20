import asyncio
import logging
from collections import deque
from collections.abc import Coroutine
from typing import Optional

from asynch.connection import Connection, connect
from asynch.proto import constants

logger = logging.getLogger(__name__)


class PoolError(Exception):
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
        self._closed: Optional[bool] = None
        self._loop = (
            loop if isinstance(loop, asyncio.AbstractEventLoop) else asyncio.get_running_loop()
        )
        self._lock = asyncio.Lock()
        self._acquired_connections: deque[Connection] = deque(maxlen=maxsize)
        self._free_connections: deque[Connection] = deque(maxlen=maxsize)

    async def __aenter__(self) -> "Pool":
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def __repr__(self):
        cls_name = self.__class__.__name__
        prefix = (
            f"<{cls_name} object at 0x{id(self):x}: "
            f"minsize={self._minsize}, "
            f"maxsize={self._maxsize}"
        )
        return f"{prefix}>"

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def minsize(self) -> int:
        return self._minsize

    @property
    def freesize(self) -> int:
        return len(self._free_connections)

    @property
    def size(self) -> int:
        return self.freesize + len(self._used)

    @property
    def cond(self) -> asyncio.Condition:
        return self._cond

    @property
    def closed(self) -> Optional[bool]:
        """Returns the connection close status.

        If the return value is None,
        the connection was only created,
        but neither opened or closed.

        :returns: the connection close status
        :rtype: None | bool
        """

        return self._closed

    def get_connections(self) -> int:
        """Returns the current number of connections in the pool.

        The current size of the pool is the sum of
        the acquired and free connections.

        :return: the number of connections in the pool
        :rtype: int
        """

        return self.get_acquired_connections() + self.get_free_connections()

    def get_acquired_connections(self) -> int:
        """Returns the number of connections acquired from the pool.

        A connection is acquired when `pool.connection()` is invoked.

        :return: the number of connections in the pool
        :rtype: int
        """

        return len(self._acquired_connections)

    def get_free_connections(self) -> int:
        """Returns the number of free connections in the pool.

        A free connection is immediately available for acquisition.

        :return: the number of connections in the pool
        :rtype: int
        """

        return len(self._free_connections)

    async def _create_connection(self) -> None:
        """Creates a connection to a ClickHouse server.

        The connection can be created while the number of connections
        (pool size) does not exceed the the maximum value.
        Otherwise, the `PoolError` exception is raised.

        :raises PoolError: pool.get_connections() >= pool.maxsize
        :return: None
        """

        pool_size, maxsize = self.get_connections(), self._maxsize
        if pool_size >= maxsize:
            raise PoolError(f"cannot exceed the maximum size of the {self}")

        conn = await connect(**self._connection_kwargs)
        self._free_connections.append(conn)

    async def _acquire_connection(self) -> Connection:
        """Returns a free connection from the pool.

        This method does not create a connection
        when free connections are None or exhausted

        :raises PoolError: no free connection in the pool
        :return: a free connection from the pool
        :rtype: Connection
        """

        async with self._lock:
            if not self._free_connections:
                raise PoolError(f"no free connection in the {self}")
            conn = self._free_connections.popleft()
            self._acquired_connections.append(conn)
            return conn

    async def connection(self) -> Connection:
        """Get a connection from the pool.

        :raises PoolError: if a connection cannot be acquired
        :return: a free connection from the pool
        :rtype: Connection
        """

        if not self._free_connections:
            to_create = min(self.minsize, self.maxsize - self.get_connections())
            tasks: list[asyncio.Task] = [
                asyncio.create_task(self._create_connection()) for _ in range(to_create)
            ]
            if tasks:
                await asyncio.gather(*tasks)
        return await self._acquire_connection()

    async def startup(self) -> "Pool":
        """Initialise the pool.

        The pool is filled with the `minsize` number of connections.
        """

        async with self._lock:
            ctasks: list[asyncio.Task] = [
                asyncio.create_task(self._create_connection()) for _ in range(self.minsize)
            ]
            await asyncio.gather(*ctasks)
        return self

    async def shutdown(self) -> None:
        """Close the pool.

        This method closes consequently free connections first.
        Then it does the same for the acquired/active connections.
        """

        async with self._lock:
            while self._free_connections:
                conn = self._free_connections.popleft()
                await conn.close()
            while self._acquired_connections:
                conn = self._acquired_connections.popleft()
                await conn.close()
            self._closed = True

    async def release(self, connection: Connection):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """

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
        while self.freesize < self.minsize and self.size <= self.maxsize:
            await self.init_one_connection()

    async def init_one_connection(self):
        conn = await connect(**self._connection_kwargs)
        self._free_connections.append(conn)
        self._cond.notify()

    async def clear(self) -> None:
        """Closes all free connections in the pool."""

        async with self._cond:
            while self._free_connections:
                conn = self._free_connections.popleft()
                await conn.close()
            self._cond.notify()

    async def wait_closed(self):
        """Waits for closing all connections in the pool."""

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
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """

        if self._closed:
            return
        self._closing = True

    async def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in self._used:
            await conn.close()
            self._terminated.add(conn)

        self._used.clear()


def create_pool(
    minsize: int = constants.POOL_MIN_SIZE,
    maxsize: int = constants.POOL_MAX_SIZE,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
):
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

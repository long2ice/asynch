import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from asynch.connection import Connection, connect
from asynch.errors import AsynchPoolError
from asynch.proto import constants
from asynch.proto.models.enums import PoolStatus

logger = logging.getLogger(__name__)


class Pool(asyncio.AbstractServer):
    def __init__(
        self,
        minsize: int = constants.POOL_MIN_SIZE,
        maxsize: int = constants.POOL_MAX_SIZE,
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
        self._sem = asyncio.Semaphore(maxsize)
        self._lock = asyncio.Lock()
        self._acquired_connections: deque[Connection] = deque(maxlen=maxsize)
        self._free_connections: deque[Connection] = deque(maxlen=maxsize)
        self._opened: Optional[bool] = None
        self._closed: Optional[bool] = None

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

    async def _create_connection(self) -> None:
        pool_size, maxsize = self.connections, self.maxsize
        if pool_size == maxsize:
            raise AsynchPoolError(f"{self} is already full")
        if pool_size > maxsize:
            raise AsynchPoolError(f"{self} is overburden")

        conn = await connect(**self._connection_kwargs)
        self._free_connections.append(conn)

    async def _acquire_connection(self) -> Connection:
        if not self._free_connections:
            raise AsynchPoolError(f"no free connection in {self}")

        conn = self._free_connections.popleft()
        self._acquired_connections.append(conn)
        return conn

    async def _release_connection(self, conn: Connection) -> None:
        if conn not in self._acquired_connections:
            raise AsynchPoolError(f"the connection {conn} does not belong to {self}")

        self._acquired_connections.remove(conn)
        self._free_connections.append(conn)

    async def _init_connections(self, n: Optional[int] = None) -> None:
        to_create = n if n is not None else self.minsize
        if to_create < 0:
            msg = f"cannot create ({to_create}) negative connections for {self}"
            raise ValueError(msg)
        if to_create == 0:
            return
        if (self.connections + to_create) > self.maxsize:
            msg = f"cannot create {to_create} connections to exceed the size of {self}"
            raise AsynchPoolError(msg)
        tasks: list[asyncio.Task] = [
            asyncio.create_task(self._create_connection()) for _ in range(to_create)
        ]
        await asyncio.wait(fs=tasks)

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Connection]:
        """Get a connection from the pool.

        If requested more connections than the pool can provide,
        the pool gets blocked until a connection comes back.

        :raises AsynchPoolError: if a connection cannot be acquired or released

        :return: a free connection from the pool
        :rtype: Connection
        """

        async with self._sem:
            async with self._lock:
                if not self._free_connections:
                    conns, maxsize = self.connections, self.maxsize
                    avail = maxsize - conns
                    if (maxsize - conns) < 0:
                        msg = (
                            f"the number of pool connections ({conns}) "
                            f"exceeds the pool maxsize ({maxsize}) for {self}"
                        )
                        raise AsynchPoolError(msg)
                    to_create = min(self.minsize, avail)
                    await self._init_connections(to_create)
                conn = await self._acquire_connection()
            try:
                yield conn
            finally:
                async with self._lock:
                    await self._release_connection(conn)

    async def startup(self) -> "Pool":
        """Initialise the pool.

        When entering the context,
        the pool get filled with connections
        up to the pool `minsize` value.
        """

        async with self._lock:
            if self._opened:
                return self
            await self._init_connections(self.minsize)
            self._opened = True
            if self._closed:
                self._closed = False
        return self

    async def shutdown(self) -> None:
        """Close the pool.

        This method closes consequently free connections first.
        Then it does the same for the acquired connections.
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


async def create_pool(
    minsize: int = constants.POOL_MIN_SIZE,
    maxsize: int = constants.POOL_MAX_SIZE,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs,
) -> Pool:
    """Returns an initiated connection pool.

    The initiated pool means it is filled with `minsize` connections.

    Equivalent to:
    1. pool = Pool(...)
    2. await pool.startup()
    3. return pool

    Do not forget to `await pool.shutdown()` for resource clean-up.

    :param minsize int: the minimum number of connections in the pool
    :param maxsize int: the maximum number of connections in the pool
    :param loop Optional[asyncio.AbstractEventLoop]: an event loop (asyncio.get_running_loop() by default)
    :param kwargs dict: connection settings

    :return: a connection pool object
    :rtype: Pool
    """

    pool = Pool(
        minsize=minsize,
        maxsize=maxsize,
        loop=loop,
        **kwargs,
    )
    await pool.startup()
    return pool

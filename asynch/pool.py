import asyncio
import logging
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Optional

from asynch.connection import Connection
from asynch.errors import AsynchPoolError
from asynch.proto import constants
from asynch.proto.models.enums import PoolStatus

logger = logging.getLogger(__name__)


class Pool:
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
        self._opened: bool = False
        self._closed: bool = False

    async def __aenter__(self) -> "Pool":
        await self.startup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.shutdown()

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        status = self.status
        return (
            f"<{cls_name}(minsize={self._minsize}, maxsize={self._maxsize})"
            f" object at 0x{id(self):x}; status: {status}>"
        )

    @property
    def opened(self) -> bool:
        """Returns True if the pool is opened.

        :returns: the pool open status
        :rtype: bool
        """

        return self._opened

    @property
    def closed(self) -> bool:
        """Return True if the pool is closed.

        :returns: the pool close status
        :rtype: bool
        """

        return self._closed

    @property
    def status(self) -> str:
        """Return the status of the pool.

        :raise AsynchPoolError: an unresolved pool state.
        :return: the Pool object status
        :rtype: str (PoolStatus StrEnum)
        """

        if not (self._opened or self._closed):
            return PoolStatus.created
        if self._opened and not self._closed:
            return PoolStatus.opened
        if self._closed and not self._opened:
            return PoolStatus.closed
        raise AsynchPoolError(f"{self} is in an unknown state")

    @property
    def acquired_connections(self) -> int:
        """Return the number of connections acquired from the pool.

        A connection is acquired when the `pool.connection()` is invoked.

        :return: the number of connections requested from the pool
        :rtype: int
        """

        return len(self._acquired_connections)

    @property
    def free_connections(self) -> int:
        """Return the number of free connections in the pool.

        :return: the number of free connections in the pool
        :rtype: int
        """

        return len(self._free_connections)

    @property
    def _pool_size(self) -> int:
        """Return the number of connections associated with the pool.

        This number is the sum of the acquired and free connections.
        So this sum may be interpreted as the current size of the pool
        or the number of connections associated with the pool and so on.

        :return: the number of connections related to the pool
        :rtype: int
        """

        return self.acquired_connections + self.free_connections

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def minsize(self) -> int:
        return self._minsize

    async def _create_connection(self) -> None:
        if self._pool_size == self._maxsize:
            raise AsynchPoolError(f"{self} is already full")
        if self._pool_size > self._maxsize:
            raise AsynchPoolError(f"{self} is overburden")

        conn = Connection(**self._connection_kwargs)
        await conn.connect()

        try:
            await conn.ping()
            self._free_connections.append(conn)
        except ConnectionError as e:
            msg = f"failed to create a {conn} for {self}"
            raise AsynchPoolError(msg) from e

    def _pop_connection(self) -> Connection:
        if not self._free_connections:
            raise AsynchPoolError(f"no free connection in {self}")
        return self._free_connections.popleft()

    async def _get_fresh_connection(self) -> Optional[Connection]:
        while self._free_connections:
            conn = self._pop_connection()
            with suppress(ConnectionError):
                await conn._refresh()
                return conn
        return None

    async def _acquire_connection(self) -> Connection:
        if conn := await self._get_fresh_connection():
            self._acquired_connections.append(conn)
            return conn

        await self._create_connection()
        conn = self._pop_connection()
        self._acquired_connections.append(conn)
        return conn

    async def _release_connection(self, conn: Connection) -> None:
        if conn not in self._acquired_connections:
            raise AsynchPoolError(f"the connection {conn} does not belong to {self}")

        self._acquired_connections.remove(conn)
        try:
            await conn._refresh()
        except ConnectionError as e:
            msg = f"the {conn} is invalidated"
            raise AsynchPoolError(msg) from e

        self._free_connections.append(conn)

    async def _init_connections(self, n: int, *, strict: bool = False) -> None:
        if n < 0:
            msg = f"cannot create a negative number ({n}) of connections for {self}"
            raise ValueError(msg)
        if (self._pool_size + n) > self.maxsize:
            msg = (
                f"{self} has the {self._pool_size} connections, "
                f"adding {n} will exceed its maxsize ({self.maxsize})"
            )
            raise AsynchPoolError(msg)
        if not n:
            return

        # it is possible that the `_create_connection` may not create `n` connections
        tasks: list[asyncio.Task] = [
            asyncio.create_task(self._create_connection()) for _ in range(n)
        ]
        # that is why possible exceptions from the `_create_connection` are also gathered
        if strict and any(
            i
            for i in await asyncio.gather(*tasks, return_exceptions=True)
            if isinstance(i, Exception)
        ):
            msg = f"failed to create the {n} connection(s) for the {self}"
            raise AsynchPoolError(msg)

    async def _ensure_minsize_connections(self, *, strict: bool = False) -> None:
        if (gap := self.minsize - self._pool_size) > 0:
            await self._init_connections(gap, strict=strict)

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
                conn = await self._acquire_connection()
            try:
                yield conn
            finally:
                async with self._lock:
                    try:
                        await self._release_connection(conn)
                    except AsynchPoolError as e:
                        logger.warning(e)
                    await self._ensure_minsize_connections(strict=True)

    async def startup(self) -> "Pool":
        """Initialise the pool.

        When entering the context,
        the pool get filled with connections
        up to the pool `minsize` value.

        :return: a pool object with `minsize` opened connections
        :rtype: Pool
        """

        async with self._lock:
            if self._opened:
                return self
            # If we cannot create the minsize connections here,
            # the Pool does not meet the minsize requirement.
            await self._init_connections(self.minsize, strict=True)
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

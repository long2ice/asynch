from __future__ import annotations

from asynch.cursors import Cursor
from asynch.errors import NotSupportedError
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.models.enums import ConnectionStatus
from asynch.proto.utils.dsn import parse_dsn


class Connection:
    def __init__(
        self,
        dsn: str | None = None,
        user: str = constants.DEFAULT_USER,
        password: str = constants.DEFAULT_PASSWORD,
        host: str = constants.DEFAULT_HOST,
        port: int | None = None,
        database: str = constants.DEFAULT_DATABASE,
        cursor_cls=Cursor,
        echo: bool = False,
        stack_track: bool = False,
        **kwargs,
    ):
        if dsn:
            config = parse_dsn(dsn)
            self._connection = ProtoConnection(**config, stack_track=stack_track, **kwargs)
            user = config.get("user", None) or user
            password = config.get("password", None) or password
            host = config.get("host", None) or host
            database = config.get("database", None) or database
        else:
            self._connection = ProtoConnection(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                stack_track=stack_track,
                **kwargs,
            )
        # The proto connection resolves an unspecified port from the scheme
        # (9440 when secure, 9000 otherwise), so read the effective one back
        # instead of second-guessing it here.
        port = self._connection.hosts[0][1]
        self._dsn = dsn
        # dsn parts
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        # connection additional settings
        self._opened: bool = False
        self._closed: bool = False
        self._cursor_cls = cursor_cls
        self._connection_kwargs = kwargs
        self._echo = echo

    async def __aenter__(self) -> Connection:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        status = self.status
        return f"<{cls_name} object at 0x{id(self):x}; status: {status}>"

    @property
    def opened(self) -> bool | None:
        """Return True if the connection is opened.

        :returns: the connection open status
        :rtype: bool
        """

        return self._opened

    @property
    def last_query(self):
        """Statistics for the most recent query on this connection.

        Exposes what the server reported while the query ran: `elapsed`
        seconds, a `progress` counter (rows/bytes read) and `profile_info`
        (including `rows_before_limit`). None before the first query.

        :return: the QueryInfo of the last executed query, if any
        """

        return self._connection.last_query

    @property
    def closed(self) -> bool:
        """Return True if the connection is closed.

        :returns: the connection close status
        :rtype: bool
        """

        return self._closed

    @property
    def status(self) -> str:
        """Return the status of the connection.

        :raise ConnectionError: an unresolved connection state
        :return: the Connection object status
        :rtype: str (ConnectionStatus StrEnum)
        """

        if not (self._opened or self._closed):
            return ConnectionStatus.created
        if self._opened and not self._closed:
            return ConnectionStatus.opened
        if self._closed and not self._opened:
            return ConnectionStatus.closed
        raise ConnectionError(f"{self} is in an unknown state")

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def user(self) -> str:
        return self._user

    @property
    def password(self) -> str:
        return self._password

    @property
    def database(self) -> str:
        return self._database

    @property
    def echo(self) -> bool:
        return self._echo

    async def close(self) -> None:
        """Close the connection."""

        if self._closed:
            return
        if self._opened:
            await self._connection.disconnect()
        self._opened = False
        self._closed = True

    async def commit(self):
        raise NotSupportedError

    async def connect(self) -> None:
        if self._opened:
            return
        await self._connection.connect()
        self._opened = True
        if self._closed:
            self._closed = False

    def cursor(self, cursor: type[Cursor] | None = None, *, echo: bool = False) -> Cursor:
        """Return the cursor object for the connection.

        When a parameter is interpreted as True,
        it takes precedence over the corresponding default value.
        If the `cursor` is None, but the `echo` is True,
        then a default Cursor instance will be created
        with echoing even if the `self.echo` returns False.

        :param cursor Optional[type[Cursor]]: Cursor factory class
        :param echo bool: to override the `Connection.echo` parameter for a cursor

        :return: a cursor object of the given connection
        :rtype: Cursor
        """

        cursor_cls = cursor or self._cursor_cls
        return cursor_cls(self, echo or self.echo)

    async def ping(self) -> None:
        """Check the connection liveliness.

        :raises ConnectionError: if ping() has failed
        :return: None
        """

        if not await self._connection.ping():
            msg = f"Ping has failed for {self}"
            raise ConnectionError(msg)

    async def cancel(self) -> bool:
        """Ask the server to stop the query running on this connection.

        Meant to be called from a different task than the one awaiting the
        query: that task then finishes early. The connection is drained to the
        end of the stream and stays usable afterwards.

        Does nothing if no query is running.

        :return: True if a query was cancelled
        """

        return await self._connection.cancel()

    async def is_live(self) -> bool:
        """Report whether the connection is still usable.

        Unlike `_refresh`, this never reconnects and never raises: a caller
        that holds a pool of connections wants to discard a dead one, not
        resurrect it in place.

        :return: True if the connection is opened and answers a ping
        """

        if self.status != ConnectionStatus.opened:
            return False
        try:
            await self.ping()
        except ConnectionError:
            return False
        return True

    async def _refresh(self) -> None:
        """Refresh the connection.

        Attempting to ping and if failed,
        then trying to connect again.
        If the reconnection does not work,
        an Exception is propagated.

        :raises ConnectionError:
            1. refreshing created, i.e., not opened connection
            2. refreshing already closed connection

        :return: None
        """

        if self.status == ConnectionStatus.created:
            msg = f"the {self} is not opened to be refreshed"
            raise ConnectionError(msg)
        if self.status == ConnectionStatus.closed:
            msg = f"the {self} is already closed"
            raise ConnectionError(msg)

        try:
            await self.ping()
        except ConnectionError:
            # `connect()` returns early while `_opened` is set, so the socket
            # has to be torn down first - otherwise the reconnect is a no-op
            # and the dead connection is handed back to the caller.
            await self.close()
            self._closed = False
            await self.connect()

    async def rollback(self):
        raise NotSupportedError

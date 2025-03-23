from typing import Optional

from asynch.cursors import Cursor
from asynch.errors import NotSupportedError
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.models.enums import ConnectionStatus
from asynch.proto.utils.dsn import parse_dsn


class Connection:
    def __init__(
        self,
        dsn: Optional[str] = None,
        user: str = constants.DEFAULT_USER,
        password: str = constants.DEFAULT_PASSWORD,
        host: str = constants.DEFAULT_HOST,
        port: int = constants.DEFAULT_PORT,
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
            port = config.get("port", None) or port
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
        self._dsn = dsn
        # dsn parts
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        # connection additional settings
        self._opened: Optional[bool] = None
        self._closed: Optional[bool] = None
        self._cursor_cls = cursor_cls
        self._connection_kwargs = kwargs
        self._echo = echo

    async def __aenter__(self) -> "Connection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        status = self.status
        return f"<{cls_name} object at 0x{id(self):x}; status: {status}>"

    @property
    def opened(self) -> Optional[bool]:
        """Returns the connection open status.

        If the return value is None,
        the connection was only created,
        but neither opened or closed.

        :returns: the connection open status
        :rtype: None | bool
        """

        return self._opened

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

    @property
    def status(self) -> str:
        """Return the status of the connection.

        If conn.connected is None and conn.closed is None,
        then the connection is in the "created" state.
        It was neither opened nor closed.

        When executing `async with conn: ...`,
        the `conn.opened` is True and `conn.closed` is None.
        When leaving the context, the `conn.closed` is True
        and the `conn.opened` is False.

        :raise ConnectionError: an unresolved connection state
        :return: the Connection object status
        :rtype: str (ConnectionStatus StrEnum)
        """

        opened, closed = self._opened, self._closed
        if opened is None and closed is None:
            return ConnectionStatus.created
        if opened and not closed:
            return ConnectionStatus.opened
        if closed and not opened:
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

    def cursor(self, cursor: Optional[type[Cursor]] = None, *, echo: bool = False) -> Cursor:
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

        status = self.status
        if status == ConnectionStatus.created:
            msg = f"the {self} is not opened to be refreshed"
            raise ConnectionError(msg)
        if status == ConnectionStatus.closed:
            msg = f"the {self} is already closed"
            raise ConnectionError(msg)

        try:
            await self.ping()
        except ConnectionError:
            await self.connect()

    async def rollback(self):
        raise NotSupportedError

from typing import Any, Dict, Optional

from asynch.cursors import Cursor
from asynch.errors import NotSupportedError
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.connection import get_default_port
from asynch.proto.models.enums import ConnectionStatus
from asynch.proto.utils.dsn import parse_dsn


class Connection:
    def __init__(
        self,
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        cursor_cls=Cursor,
        echo: bool = False,
        stack_track: bool = False,
        secure: bool = False,
        **kwargs,
    ):
        self._proto_kwargs = self._get_proto_connection_kwargs(
            dsn,
            user,
            password,
            host,
            port,
            database,
            secure=secure,
            stack_track=stack_track,
            **kwargs,
        )
        self._connection = ProtoConnection(**self._proto_kwargs)
        self._cursor_cls = cursor_cls
        self._echo = echo
        # connection additional settings
        self._opened: bool = False
        self._closed: bool = False

    @staticmethod
    def _get_proto_connection_kwargs(
        dsn: Optional[str],
        user: Optional[str],
        password: Optional[str],
        host: Optional[str],
        port: Optional[int],
        database: Optional[str],
        *,
        secure: bool,
        stack_track: bool,
        **kwargs,
    ) -> Dict[str, Any]:
        conninfo: Dict[str, Any] = {}
        if dsn:
            config = parse_dsn(dsn)
            conninfo = {**conninfo, **config}
            # Parameters suppercede DSN parts!
            # If a param is given and it is not False-evaliuated, this is it,
            # otherwise the corresponding DSN part is used OR the param itself.
            user = user or config.get("user", constants.DEFAULT_USER)
            password = password or config.get("password", constants.DEFAULT_PASSWORD)
            host = host or config.get("host", constants.DEFAULT_HOST)
            port = port or config.get("port", get_default_port(secure=secure))
            database = database or config.get("database", constants.DEFAULT_DATABASE)
            secure = secure or config.get("secure", secure)
        return {
            **conninfo,
            "user": user,
            "password": password,
            "host": host,
            "port": port,
            "database": database,
            "secure": secure,
            "stack_track": stack_track,
            **kwargs,
        }

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
        """Return True if the connection is opened.

        :returns: the connection open status
        :rtype: bool
        """

        return self._opened

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
    def host(self) -> Optional[str]:
        return self._proto_kwargs.get("host")

    @property
    def port(self) -> Optional[int]:
        return self._proto_kwargs.get("port")

    @property
    def user(self) -> Optional[str]:
        return self._proto_kwargs.get("user")

    @property
    def password(self) -> Optional[str]:
        return self._proto_kwargs.get("password")

    @property
    def database(self) -> Optional[str]:
        return self._proto_kwargs.get("database")

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

        if self.status == ConnectionStatus.created:
            msg = f"the {self} is not opened to be refreshed"
            raise ConnectionError(msg)
        if self.status == ConnectionStatus.closed:
            msg = f"the {self} is already closed"
            raise ConnectionError(msg)

        try:
            await self.ping()
        except ConnectionError:
            await self.connect()

    async def rollback(self):
        raise NotSupportedError

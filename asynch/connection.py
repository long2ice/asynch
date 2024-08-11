from typing import Optional

from asynch import errors
from asynch.cursors import Cursor
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.models.enums import ConnectionStatuses
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
        self._is_connected: Optional[bool] = None
        self._is_closed: Optional[bool] = None
        self._echo = echo
        self._cursor_cls = cursor_cls
        self._connection_kwargs = kwargs

    async def __aenter__(self) -> "Connection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def __repr__(self):
        cls_name = self.__class__.__name__
        prefix = f"<{cls_name} object at 0x{id(self):x}; status: "
        if self.connected:
            prefix += ConnectionStatuses.opened
        elif self.closed:
            prefix += ConnectionStatuses.closed
        else:
            prefix += ConnectionStatuses.created
        return f"{prefix}>"

    @property
    def connected(self) -> Optional[bool]:
        """Returns the connection open status.

        If the return value is None,
        the connection was only created,
        but neither opened or closed.

        :returns: the connection open status
        :rtype: None | bool
        """

        return self._is_connected

    @property
    def closed(self) -> Optional[bool]:
        """Returns the connection close status.

        If the return value is None,
        the connection was only created,
        but neither opened or closed.

        :returns: the connection close status
        :rtype: None | bool
        """

        return self._is_closed

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
        if self._is_connected:
            await self._connection.disconnect()
            self._is_connected = False
            self._is_closed = True

    async def commit(self):
        raise errors.NotSupportedError

    async def rollback(self):
        raise errors.NotSupportedError

    async def connect(self) -> None:
        if not self._is_connected:
            await self._connection.connect()
            self._is_connected = True
            if self._is_closed is True:
                self._is_closed = False

    def cursor(self, cursor: Optional[Cursor] = None, *, echo: bool = False) -> Cursor:
        cursor_cls = cursor or self._cursor_cls
        return cursor_cls(self, self._echo or echo)

    async def ping(self):
        await self._connection.ping()


async def connect(
    dsn: Optional[str] = None,
    user: str = constants.DEFAULT_USER,
    password: str = constants.DEFAULT_PASSWORD,
    host: str = constants.DEFAULT_HOST,
    port: int = constants.DEFAULT_PORT,
    database: str = constants.DEFAULT_DATABASE,
    cursor_cls=Cursor,
    echo: bool = False,
    **kwargs,
) -> Connection:
    """Open the connection to a ClickHouse server.

    Equivalent to the following steps:
    1. conn = Connection(...)  # init a Connection instance
    2. conn.connect()  # connect to a ClickHouse instance

    :return: the open connection
    :rtype: Connection
    """

    conn = Connection(
        dsn=dsn,
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        cursor_cls=cursor_cls,
        echo=echo,
        **kwargs,
    )
    await conn.connect()
    return conn

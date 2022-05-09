import ssl
from typing import Type
from urllib.parse import parse_qs, unquote, urlparse

from asynch import errors
from asynch.cursors import Cursor
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.utils.compat import asbool


class Connection:
    def __init__(
        self,
        dsn: str = None,
        host: str = "127.0.0.1",
        port: int = 9000,
        database: str = constants.DEFAULT_DATABASE,
        user: str = constants.DEFAULT_USER,
        password: str = constants.DEFAULT_PASSWORD,
        cursor_cls=Cursor,
        echo=False,
        stack_track=False,
        **kwargs,
    ):
        self._dsn = dsn
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._connection_kwargs = kwargs
        self._is_closed = False
        self._echo = echo
        self._cursor_cls = cursor_cls
        self._connected = False
        if dsn:
            self._connection = ProtoConnection(
                **self._parse_dsn(dsn), stack_track=stack_track, **kwargs
            )
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

    def __repr__(self):
        return "<connection object at 0x{0:x}; closed: {1:}>".format(id(self), self._is_closed)

    @property
    def connected(self):
        return self._connected

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def database(self):
        return self._database

    @property
    def echo(self):
        return self._echo

    async def close(self):
        if self._is_closed:
            return
        await self._connection.disconnect()
        self._is_closed = True

    async def commit(self):
        raise errors.NotSupportedError

    async def rollback(self):
        raise errors.NotSupportedError

    async def connect(self):
        if self._connected:
            return
        await self._connection.connect()
        self._connected = True

    def cursor(self, cursor: Type[Cursor] = None) -> Cursor:
        cursor_cls = cursor or self._cursor_cls
        return cursor_cls(self, self._echo)

    def _parse_dsn(self, url):
        """
        Return a client configured from the given URL.

        For example::

            clickhouse://[user:password]@localhost:9000/default
            clickhouses://[user:password]@localhost:9440/default

        Three URL schemes are supported:
            clickhouse:// creates a normal TCP socket connection
            clickhouses:// creates a SSL wrapped TCP socket connection

        Any additional querystring arguments will be passed along to
        the Connection class's initializer.
        """
        url = urlparse(url)

        settings = {}
        kwargs = {"host": url.hostname}

        if url.port is not None:
            self._port = kwargs["port"] = url.port

        path = url.path.replace("/", "", 1)
        if path:
            self._database = kwargs["database"] = path

        if url.username is not None:
            self._user = kwargs["user"] = unquote(url.username)

        if url.password is not None:
            self._password = kwargs["password"] = unquote(url.password)

        if url.scheme == "clickhouses":
            kwargs["secure"] = True

        compression_algs = {"lz4", "lz4hc", "zstd"}
        timeouts = {"connect_timeout", "send_receive_timeout", "sync_request_timeout"}

        for name, value in parse_qs(url.query).items():
            if not value or not len(value):
                continue

            value = value[0]

            if name == "compression":
                value = value.lower()
                if value in compression_algs:
                    kwargs[name] = value
                else:
                    kwargs[name] = asbool(value)

            elif name == "secure":
                kwargs[name] = asbool(value)

            elif name == "client_name":
                kwargs[name] = value

            elif name in timeouts:
                kwargs[name] = float(value)

            elif name == "compress_block_size":
                kwargs[name] = int(value)

            # ssl
            elif name == "verify":
                kwargs[name] = asbool(value)
            elif name == "ssl_version":
                kwargs[name] = getattr(ssl, value)
            elif name in ["ca_certs", "ciphers"]:
                kwargs[name] = value
            elif name == "alt_hosts":
                kwargs["alt_hosts"] = value
            else:
                settings[name] = value

        if settings:
            kwargs["settings"] = settings

        return kwargs


async def connect(
    dsn: str = None,
    host: str = "127.0.0.1",
    port: int = 9000,
    database: str = "default",
    user: str = "default",
    password: str = "",
    cursor_cls=Cursor,
    echo=False,
    **kwargs,
) -> Connection:
    conn = Connection(dsn, host, port, database, user, password, cursor_cls, echo=echo, **kwargs)
    await conn.connect()
    return conn

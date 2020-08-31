import ssl
from urllib.parse import parse_qs, unquote, urlparse

from asynch import errors
from asynch.cursors import Cursor
from asynch.proto.connection import Connection as ProtoConnection
from asynch.proto.utils.compat import asbool


class Connection:
    def __init__(
        self,
        dsn: str = None,
        host: str = "127.0.0.1",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        cursor_cls=Cursor,
        echo=False,
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
        if dsn:
            self.connection = ProtoConnection(**self._parse_dsn(dsn), **kwargs)
        else:
            self.connection = ProtoConnection(
                host=host, port=port, database=database, user=user, password=password, **kwargs
            )

    def __repr__(self):
        return "<connection object at 0x{0:x}; closed: {1:}>".format(id(self), self._is_closed)

    async def close(self):
        if self._is_closed:
            return
        await self.connection.disconnect()
        self._is_closed = True

    async def commit(self):
        raise errors.NotSupportedError

    async def rollback(self):
        raise errors.NotSupportedError

    def cursor(self, cursor: Cursor = None) -> Cursor:
        cursor_cls = cursor or self._cursor_cls
        return cursor_cls(connection=self)

    @classmethod
    def _parse_dsn(cls, url):
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
            kwargs["port"] = url.port

        path = url.path.replace("/", "", 1)
        if path:
            kwargs["database"] = path

        if url.username is not None:
            kwargs["user"] = unquote(url.username)

        if url.password is not None:
            kwargs["password"] = unquote(url.password)

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

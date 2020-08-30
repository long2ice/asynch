from asynch import errors
from asynch.cursors import Cursor
from asynch.proto import constants
from asynch.proto.connection import Connection as ProtoConnection


class Connection:
    def __init__(
        self,
        dsn: str = None,
        host: str = "127.0.0.1",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        cursor_class=Cursor,
        **kwargs,
    ):
        self.dsn = dsn
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.connection_kwargs = kwargs
        self.cursors = []
        self.is_closed = False
        self.cursor_class = cursor_class

    def __repr__(self):
        return "<connection object at 0x{0:x}; closed: {1:}>".format(id(self), self.is_closed)

    async def close(self):
        await self.conn

        self.is_closed = True

    async def commit(self):
        raise errors.NotSupportedError

    async def rollback(self):
        raise errors.NotSupportedError

    async def cursor(self) -> Cursor:
        return Cursor(connection=self)

    async def execute(self):
        pass

    async def execute_iter(self):
        pass

    def __enter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

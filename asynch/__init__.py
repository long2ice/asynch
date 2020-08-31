from asynch.connection import Connection
from asynch.cursors import Cursor


def connect(
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
    return Connection(dsn, host, port, database, user, password, cursor_cls, echo, **kwargs)


async def create_pool():
    pass

from asynch.connection import Connection
from asynch.cursors import Cursor, DictCursor
from asynch.errors import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,  # noqa: A004  # the name is mandated by PEP 249
)
from asynch.pool import Pool

# PEP 249 module globals. `paramstyle` is "pyformat": queries interpolate
# %(name)s placeholders, as `Connection.substitute_params` does.
# threadsafety 1 = the module may be shared between threads, connections may
# not: a Connection owns one socket and one parse buffer.
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"


def connect(dsn: str | None = None, **kwargs) -> Connection:
    """Create a Connection, as PEP 249 spells it.

    The connection is returned unopened; `await`ing it is done by
    `async with` or an explicit `await conn.connect()`.
    """
    return Connection(dsn=dsn, **kwargs)


__all__ = [
    "Connection",
    "Cursor",
    "DataError",
    "DatabaseError",
    "DictCursor",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "Pool",
    "ProgrammingError",
    "Warning",
    "apilevel",
    "connect",
    "paramstyle",
    "threadsafety",
]

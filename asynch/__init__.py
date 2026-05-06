from asynch.connection import Connection
from asynch.cursors import Cursor, DictCursor
from asynch.pool import Pool

# Import PEP 249 type objects and constructors
from asynch.dbapi_types import (
    STRING, BINARY, NUMBER, DATETIME, ROWID,
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary
)

# Import PEP 249 exceptions
from asynch.errors import (
    Warning,
    Error,
    InterfaceError,
    DatabaseError,
    DataError,
    OperationalError,
    IntegrityError,
    InternalError,
    ProgrammingError,
    NotSupportedError,
)


# PEP 249 module globals
apilevel = "2.0"
threadsafety = 1  # module shareable; connections are per-event-loop
paramstyle = "pyformat"  # %(name)s style parameter substitution


def connect(
    dsn=None,
    user=None,
    password=None,
    host=None,
    port=None,
    database=None,
    **kwargs,
) -> Connection:
    """Create a new database connection.

    This function returns a Connection object. Note that the connection
    is not automatically opened - callers must use `await conn.connect()`
    or `async with conn` to establish the actual connection.

    Args:
        dsn: Data Source Name string (takes precedence over individual params)
        user: ClickHouse username
        password: ClickHouse password
        host: ClickHouse server host
        port: ClickHouse server port
        database: ClickHouse database name
        **kwargs: Additional connection parameters

    Returns:
        Connection: A new Connection object
    """
    return Connection(
        dsn=dsn,
        user=user,
        password=password,
        host=host,
        port=port,
        database=database,
        **kwargs,
    )


__all__ = [
    # Core classes
    "Connection", "Cursor", "DictCursor", "Pool", "connect",
    # Module globals
    "apilevel", "threadsafety", "paramstyle",
    # Type objects
    "STRING", "BINARY", "NUMBER", "DATETIME", "ROWID",
    # Type constructors
    "Date", "Time", "Timestamp", "DateFromTicks", "TimeFromTicks", "TimestampFromTicks", "Binary",
    # Exceptions
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError",
    "OperationalError", "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
]

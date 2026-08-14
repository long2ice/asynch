from enum import Enum


class _StrEnum(str, Enum):
    """A `str`-mixin enum whose members format as their values on every Python version.

    Python 3.11 changed `str()`/`format()` of mixin enums to return e.g.
    "ConnectionStatus.opened" instead of "opened"; this pins the pre-3.11
    (and `enum.StrEnum`) behavior while the project still supports 3.9/3.10.
    """

    __str__ = str.__str__
    __format__ = str.__format__  # type: ignore[assignment]


class CompressionAlgorithm(_StrEnum):
    lz4 = "lz4"
    lz4hc = "lz4hc"
    zstd = "zstd"


class ConnectionStatus(_StrEnum):
    created = "created"
    opened = "opened"
    closed = "closed"


class CursorStatus(_StrEnum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"


class PoolStatus(_StrEnum):
    created = "created"
    opened = "opened"
    closed = "closed"


class ClickhouseScheme(_StrEnum):
    clickhouse = "clickhouse"
    clickhouses = "clickhouses"

from enum import Enum


class CompressionAlgorithm(str, Enum):
    lz4 = "lz4"
    lz4hc = "lz4hc"
    zstd = "zstd"


class ConnectionStatus(str, Enum):
    created = "created"
    opened = "opened"
    closed = "closed"


class CursorStatus(str, Enum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"


class PoolStatus(str, Enum):
    created = "created"
    opened = "opened"
    closed = "closed"


class ClickhouseScheme(str, Enum):
    clickhouse = "clickhouse"
    clickhouses = "clickhouses"

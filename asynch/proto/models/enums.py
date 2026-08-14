from enum import StrEnum as _StrEnum


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

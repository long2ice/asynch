from enum import Enum


class CompressionAlgorithm(str, Enum):
    lz4 = "lz4"
    lz4hc = "lz4hc"
    zstd = "zstd"


class ConnectionStatus(str, Enum):
    created = "created"
    opened = "opened"
    closed = "closed"

    def __str__(self):
        return self.value


class CursorStatus(str, Enum):
    ready = "ready"
    running = "running"
    finished = "finished"
    closed = "closed"

    def __str__(self):
        return self.value


class PoolStatus(str, Enum):
    created = "created"
    opened = "opened"
    closed = "closed"


class ClickhouseScheme(str, Enum):
    clickhouse = "clickhouse"
    clickhouses = "clickhouses"

    def __str__(self):
        return self.value

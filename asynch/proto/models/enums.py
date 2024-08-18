from enum import Enum


class CompressionAlgorithms(str, Enum):
    lz4 = "lz4"
    lz4hc = "lz4hc"
    zstd = "zstd"


class ConnectionStatuses(str, Enum):
    created = "created"
    opened = "opened"
    closed = "closed"


class Schemes(str, Enum):
    clickhouse = "clickhouse"
    clickhouses = "clickhouses"

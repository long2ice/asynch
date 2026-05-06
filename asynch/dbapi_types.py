"""PEP 249 type objects and constructors for asynch."""
import datetime


class _DBAPITypeObject:
    """Type object for PEP 249 compliance."""

    def __init__(self, *values):
        self.values = frozenset(values)

    def __eq__(self, other):
        if isinstance(other, _DBAPITypeObject):
            return self.values == other.values
        return other in self.values

    def __repr__(self):
        return f"DBAPIType({', '.join(sorted(self.values))})"

    def __hash__(self):
        return hash(self.values)


# PEP 249 type singletons
STRING = _DBAPITypeObject(
    "String", "FixedString", "Enum8", "Enum16",
    "LowCardinality", "UUID", "IPv4", "IPv6"
)

BINARY = _DBAPITypeObject("FixedString")  # raw bytes variant

NUMBER = _DBAPITypeObject(
    "Int8", "Int16", "Int32", "Int64",
    "UInt8", "UInt16", "UInt32", "UInt64",
    "Int128", "Int256", "UInt128", "UInt256",
    "Float32", "Float64",
    "Decimal", "Decimal32", "Decimal64",
    "Decimal128", "Decimal256", "Bool"
)

DATETIME = _DBAPITypeObject("Date", "Date32", "DateTime", "DateTime64")

ROWID = _DBAPITypeObject()  # ClickHouse has no ROWID concept


# PEP 249 constructors
def Date(year, month, day):
    """Construct a date object."""
    return datetime.date(year, month, day)


def Time(hour, minute, second):
    """Construct a time object."""
    return datetime.time(hour, minute, second)


def Timestamp(year, month, day, hour, minute, second):
    """Construct a timestamp (datetime) object."""
    return datetime.datetime(year, month, day, hour, minute, second)


def DateFromTicks(ticks):
    """Construct a date object from a UNIX timestamp."""
    return datetime.date.fromtimestamp(ticks)


def TimeFromTicks(ticks):
    """Construct a time object from a UNIX timestamp."""
    return datetime.datetime.fromtimestamp(ticks).time()


def TimestampFromTicks(ticks):
    """Construct a timestamp (datetime) object from a UNIX timestamp."""
    return datetime.datetime.fromtimestamp(ticks)


def Binary(string):
    """Construct a binary object."""
    if isinstance(string, bytes):
        return string
    elif isinstance(string, str):
        return string.encode('utf-8')
    else:
        return bytes(string)
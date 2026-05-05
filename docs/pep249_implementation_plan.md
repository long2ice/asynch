# PEP 249 Implementation Plan for asynch

This document maps every PEP 249 gap against the current codebase, explains the impact, and proposes the implementation for each item.  Items are ordered by dependency — implement earlier items first.

---

## Status Summary

| Requirement | Status | Priority |
|---|---|---|
| `apilevel` module global | ❌ Missing | P0 |
| `threadsafety` module global | ❌ Missing | P0 |
| `paramstyle` module global | ❌ Missing | P0 |
| Module-level `connect()` | ❌ Missing | P0 |
| Exception classes on module | ❌ Missing | P0 |
| Type singletons (`STRING`, `NUMBER`, …) | ❌ Missing | P0 |
| Type constructors (`Date`, `Time`, …) | ❌ Missing | P0 |
| `cursor.arraysize` (public property) | ⚠️ Private only | P1 |
| `cursor.description` `type_code` mapping | ⚠️ Wrong type | P1 |
| `cursor.setoutputsize` (singular) | ⚠️ Wrong name | P1 |
| `cursor.lastrowid` | ❌ Missing | P1 |
| `cursor.callproc()` | ❌ Missing | P2 |
| `cursor.nextset()` | ❌ Missing | P2 |
| `connection.commit()` no-op | ⚠️ Raises error | P1 |
| `cursor.description` → None for non-SELECT | ⚠️ Returns list | P1 |

---

## P0 — Module Interface

### 1. Module globals (`apilevel`, `threadsafety`, `paramstyle`)

**File:** `asynch/__init__.py`

**Change:** Add three module-level constants.

```python
apilevel = "2.0"
threadsafety = 1   # module shareable; connections are per-event-loop
paramstyle = "pyformat"  # or whichever style the proto layer uses
```

**Notes on `paramstyle`:** The underlying `asynch.proto.connection.Connection.execute()` receives `args` and passes them through to ClickHouse's native protocol, which performs its own substitution. Inspect what placeholder format the proto layer actually interprets (likely `%s` / `%(name)s` from the existing clickhouse-driver heritage) and set `paramstyle` accordingly.  If no substitution is done at the Python level, set `paramstyle = "pyformat"` as a declaration and implement substitution in `Cursor.execute()`.

---

### 2. Module-level `connect()` factory

**File:** `asynch/__init__.py`

**Change:** Add a `connect` function that returns a `Connection`.

```python
from asynch.connection import Connection

def connect(
    dsn=None,
    user=...,
    password=...,
    host=...,
    port=...,
    database=...,
    **kwargs,
) -> Connection:
    return Connection(dsn=dsn, user=user, password=password,
                      host=host, port=port, database=database, **kwargs)
```

`connect()` itself does **not** open the connection (since opening is async); callers must `await conn.connect()` or use `async with conn`.  Document this deviation from the synchronous PEP 249 convention.

---

### 3. Exception classes on the module

**File:** `asynch/__init__.py`

**Change:** Re-export all PEP 249 exceptions so they are accessible as `asynch.DatabaseError`, etc.

```python
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
```

Update `__all__` to include these names.

**Note on hierarchy:** The current `InterfaceError` and `DatabaseError` inherit from `ClickHouseException → Error`.  This is technically compliant (`issubclass(InterfaceError, Error)` is `True`), but `InterfaceError` is **not** a sibling of `DatabaseError` — it inherits from the same parent.  PEP 249 requires:

```
Error
├── InterfaceError
└── DatabaseError
    ├── DataError ...
```

Consider refactoring so `InterfaceError(Error)` and `DatabaseError(Error)` are **direct** subclasses of `Error`, not routed through `ClickHouseException`.

---

### 4. Type objects and constructors

**File:** `asynch/dbapi_types.py` (new file) + `asynch/__init__.py`

Create a module that defines all PEP 249 type objects and constructors.

#### Type singleton pattern

PEP 249 requires that `type_code` values in `cursor.description` be objects that support equality comparison with `==`.  The canonical approach uses a class whose instances compare equal to other instances of the same class:

```python
class _DBAPITypeObject:
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

STRING   = _DBAPITypeObject("String", "FixedString", "Enum8", "Enum16",
                             "LowCardinality", "UUID", "IPv4", "IPv6")
BINARY   = _DBAPITypeObject("FixedString")  # raw bytes variant
NUMBER   = _DBAPITypeObject("Int8", "Int16", "Int32", "Int64",
                             "UInt8", "UInt16", "UInt32", "UInt64",
                             "Int128", "Int256", "UInt128", "UInt256",
                             "Float32", "Float64",
                             "Decimal", "Decimal32", "Decimal64",
                             "Decimal128", "Decimal256", "Bool")
DATETIME = _DBAPITypeObject("Date", "Date32", "DateTime", "DateTime64")
ROWID    = _DBAPITypeObject()  # ClickHouse has no ROWID concept
```

The `__eq__` override lets callers write `if col_type_code == NUMBER:` using either a raw ClickHouse type string or another `_DBAPITypeObject`.

#### Constructors

```python
import datetime

def Date(year, month, day):
    return datetime.date(year, month, day)

def Time(hour, minute, second):
    return datetime.time(hour, minute, second)

def Timestamp(year, month, day, hour, minute, second):
    return datetime.datetime(year, month, day, hour, minute, second)

def DateFromTicks(ticks):
    return datetime.date.fromtimestamp(ticks)

def TimeFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks).time()

def TimestampFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks)

def Binary(string):
    return bytes(string) if not isinstance(string, bytes) else string
```

Export everything from `asynch/__init__.py`.

---

## P1 — Cursor and Connection Fixes

### 5. `cursor.arraysize` — expose as public property

**File:** `asynch/cursors.py`

**Current state:** `_arraysize` is a private attribute set to `1` in `__init__`.

**Change:** Add a public property with getter and setter.

```python
@property
def arraysize(self) -> int:
    return self._arraysize

@arraysize.setter
def arraysize(self, value: int) -> None:
    self._arraysize = value
```

PEP 249 says `arraysize` must default to `1` — already satisfied.

---

### 6. `cursor.setoutputsize` — fix method name

**File:** `asynch/cursors.py`

**Current state:** Method is named `setoutputsizes` (plural, non-standard).

**Change:** Rename to `setoutputsize` (singular per PEP 249).  Keep `setoutputsizes` as a deprecated alias if backward compatibility matters.

```python
def setoutputsize(self, size, column=None):
    """Does nothing, required by DB-API 2.0."""

# Deprecated alias — remove in a future release
setoutputsizes = setoutputsize
```

---

### 7. `cursor.description` — correct `type_code` and `None` for non-SELECT

**File:** `asynch/cursors.py`

**Current state:**
- `type_code` is the raw ClickHouse type string (e.g. `"UInt64"`), not a PEP 249 type object.
- When the cursor is in `ready` state (no query executed), `description` returns `None` ✅.
- After a non-SELECT (INSERT, DDL), `_columns_with_types` ends up empty and `description` returns an empty list `[]` instead of `None`.

**Change:**

```python
from asynch.dbapi_types import STRING, BINARY, NUMBER, DATETIME, ROWID

# Map ClickHouse base type names to PEP 249 type objects
_TYPE_MAP = {
    **{t: NUMBER for t in (
        "Int8", "Int16", "Int32", "Int64",
        "UInt8", "UInt16", "UInt32", "UInt64",
        "Int128", "Int256", "UInt128", "UInt256",
        "Float32", "Float64",
        "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256",
        "Bool",
    )},
    **{t: STRING for t in (
        "String", "FixedString", "Enum8", "Enum16",
        "LowCardinality", "UUID", "IPv4", "IPv6",
    )},
    **{t: DATETIME for t in ("Date", "Date32", "DateTime", "DateTime64")},
    "Array": BINARY,
    "Map": BINARY,
    "Tuple": BINARY,
    "JSON": STRING,
}

def _ch_type_to_dbapi(ch_type_str: str):
    """Return the PEP 249 type object for a ClickHouse type string."""
    base = ch_type_str.split("(")[0].strip()  # strip e.g. "Nullable(", "LowCardinality("
    # Unwrap Nullable / LowCardinality
    for wrapper in ("Nullable", "LowCardinality"):
        if base == wrapper:
            inner = ch_type_str[len(wrapper) + 1:-1]
            return _ch_type_to_dbapi(inner)
    return _TYPE_MAP.get(base, STRING)
```

Update the `description` property:

```python
@property
def description(self):
    if self._state == CursorStatus.ready:
        return None

    columns = self._columns or []
    types = self._types or []

    if not columns:
        return None  # non-SELECT or DDL — was returning [] before

    return [
        Column(
            name,
            _ch_type_to_dbapi(type_code),  # was: raw type_code string
            None,   # display_size
            None,   # internal_size
            None,   # precision
            None,   # scale
            True,   # null_ok — ClickHouse Nullable columns can be detected but True is safe default
        )
        for name, type_code in zip(columns, types)
    ]
```

---

### 8. `cursor.lastrowid`

**File:** `asynch/cursors.py`

**Current state:** Attribute does not exist.

**Change:** Add a `lastrowid` property.  ClickHouse does not return auto-generated row IDs from INSERT, so this should always return `None`.  SQLAlchemy handles `None` gracefully.

```python
@property
def lastrowid(self):
    """Return the rowid/identity of the last inserted row, or None.

    ClickHouse does not expose row IDs, so this always returns None.
    Required by many SQLAlchemy dialects as an optional DB-API extension.
    """
    return None
```

---

### 9. `connection.commit()` — silent no-op

**File:** `asynch/connection.py`

**Current state:** `commit()` raises `NotSupportedError`.

**Why this matters:** SQLAlchemy always calls `commit()` at transaction boundaries.  Raising `NotSupportedError` breaks standard SQLAlchemy usage even when no user transaction was intended.

**Change:** Make `commit()` a no-op.

```python
async def commit(self):
    """No-op.  ClickHouse is auto-commit; there are no transactions to commit."""
```

Keep `rollback()` raising `NotSupportedError` (or make it a no-op as well, depending on how the ClickHouse SQLAlchemy dialect handles it).  Note: PEP 249 explicitly allows `rollback()` to raise `NotSupportedError` for databases without transaction support.  However, for maximum SQLAlchemy compatibility, a no-op is safer:

```python
async def rollback(self):
    """No-op.  ClickHouse does not support transactions."""
```

---

## P2 — Optional but PEP 249 Required Methods

### 10. `cursor.callproc()`

**File:** `asynch/cursors.py`

PEP 249 lists `callproc` as a required method, but explicitly states that databases which don't support stored procedures should raise `NotSupportedError`.  ClickHouse has no stored procedures.

```python
async def callproc(self, procname, parameters=None):
    """ClickHouse does not support stored procedures."""
    raise NotSupportedError("ClickHouse does not support stored procedures")
```

---

### 11. `cursor.nextset()`

**File:** `asynch/cursors.py`

ClickHouse queries always return a single result set.  PEP 249 says if the database does not support this operation, the interface should raise `NotSupportedError`, or alternatively return `None` to indicate there are no more result sets.

```python
async def nextset(self):
    """ClickHouse returns a single result set; there is no next set."""
    return None  # preferred over raising — None signals "no more sets"
```

---

## Additional Improvements

### 12. `cursor.rownumber` (SQLAlchemy optional extension)

Track current row position for scrollable cursors.

```python
@property
def rownumber(self):
    """0-based position in the result set, or None for streaming cursors."""
    return self._rownumber  # increment in fetchone/fetchmany
```

### 13. Exception classes on Connection / Cursor (optional)

PEP 249 recommends (but does not strictly require) exposing exception classes as attributes of the connection and cursor so that code can write `conn.DatabaseError`:

```python
# In Connection.__init__ or as class attributes
Error = errors.Error
DatabaseError = errors.DatabaseError
InterfaceError = errors.InterfaceError
# ...etc
```

### 14. `null_ok` in `cursor.description`

Currently hardcoded to `True`.  Improvement: inspect the ClickHouse type string for `Nullable(...)` wrapper to set `null_ok` accurately.

```python
def _is_nullable(ch_type_str: str) -> bool:
    return ch_type_str.strip().startswith("Nullable(")
```

---

## Implementation Order

1. **`asynch/dbapi_types.py`** — create new file with type objects and constructors
2. **`asynch/__init__.py`** — add `apilevel`, `threadsafety`, `paramstyle`, `connect()`, re-export exceptions and type objects
3. **`asynch/cursors.py`** — `arraysize` property, `setoutputsize`, `lastrowid`, `callproc`, `nextset`, fix `description`
4. **`asynch/connection.py`** — make `commit()` / `rollback()` no-ops

After each step, run `pytest tests/pep249/` and verify the corresponding tests go green.

---

## Files to Create / Modify

| File | Action | Summary |
|---|---|---|
| `asynch/dbapi_types.py` | Create | Type singletons + constructors |
| `asynch/__init__.py` | Modify | Add globals, `connect()`, re-exports |
| `asynch/cursors.py` | Modify | `arraysize`, `setoutputsize`, `lastrowid`, `callproc`, `nextset`, `description` fix |
| `asynch/connection.py` | Modify | `commit()` / `rollback()` as no-ops |

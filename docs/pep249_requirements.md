# Python DB-API v2.0 (PEP 249) Requirements

This document summarises every requirement imposed by [PEP 249](https://peps.python.org/pep-0249/) and the additional interface expectations of SQLAlchemy.  It is the reference against which the `tests/pep249/` and `tests/sqlalchemy/` suites are written.

---

## 1. Module-Level Interface

Every DB-API 2.0-compliant module **must** export three globals.

| Name | Type | Required values |
|---|---|---|
| `apilevel` | `str` | `"2.0"` |
| `threadsafety` | `int` | `0` – `3` (see below) |
| `paramstyle` | `str` | `"qmark"`, `"numeric"`, `"named"`, `"format"`, or `"pyformat"` |

**`threadsafety` levels**

| Value | Meaning |
|---|---|
| 0 | Threads may not share the module |
| 1 | Threads may share the module but not connections |
| 2 | Threads may share connections |
| 3 | Threads may share cursors |

**`paramstyle` values and their SQL placeholder formats**

| Value | Placeholder format |
|---|---|
| `qmark` | `WHERE name = ?` |
| `numeric` | `WHERE name = :1` |
| `named` | `WHERE name = :name` |
| `format` | `WHERE name = %s` |
| `pyformat` | `WHERE name = %(name)s` |

### Module-level `connect()` factory

The module **must** expose a `connect()` constructor that returns a `Connection` object.

```python
connection = connect(parameters...)
```

The precise signature is driver-specific; keyword arguments matching the `Connection` constructor are standard practice.

### Module-level exception classes

Every exception defined in the DB-API standard **must** be accessible as a module-level name so callers can write `except asynch.DatabaseError`.

---

## 2. Exception Hierarchy

```
Exception
├── Warning                   # Non-fatal (data truncation, etc.)
└── Error                     # Base for all errors
    ├── InterfaceError        # DB interface problems (not the DB itself)
    └── DatabaseError         # Database-side errors
        ├── DataError         # Bad data (range, division by zero)
        ├── OperationalError  # DB operation errors (disconnect, OOM)
        ├── IntegrityError    # Referential integrity violations
        ├── InternalError     # DB internal errors (invalid cursor)
        ├── ProgrammingError  # Programming mistakes (syntax, no table)
        └── NotSupportedError # Unsupported feature
```

Critical rules:
- `Warning` **must** be a subclass of `Exception`.
- `Error` **must** be a subclass of `Exception`.
- `InterfaceError` **must** be a subclass of `Error` (not just `DatabaseError`).
- `DatabaseError` **must** be a subclass of `Error`.
- All `DatabaseError` sub-exceptions must be subclasses of `DatabaseError`.

---

## 3. Type Objects and Constructors

These must be exported at module level.

### Constructors

| Name | Signature | Returns |
|---|---|---|
| `Date` | `(year, month, day)` | `datetime.date` |
| `Time` | `(hour, minute, second)` | `datetime.time` |
| `Timestamp` | `(year, month, day, hour, minute, second)` | `datetime.datetime` |
| `DateFromTicks` | `(ticks)` | `datetime.date` from a Unix timestamp |
| `TimeFromTicks` | `(ticks)` | `datetime.time` from a Unix timestamp |
| `TimestampFromTicks` | `(ticks)` | `datetime.datetime` from a Unix timestamp |
| `Binary` | `(string)` | Object representing binary data |

### Type singleton objects

These are used as the `type_code` value in `cursor.description`.  The spec requires them to be comparable with `==`, so a column may be checked as `description[i][1] == STRING`.

| Name | Represents |
|---|---|
| `STRING` | Character / text columns |
| `BINARY` | Long binary / byte columns |
| `NUMBER` | Numeric columns (int, float, decimal) |
| `DATETIME` | Date and/or time columns |
| `ROWID` | Row-ID columns |

A single type object may cover multiple underlying types (e.g. `NUMBER` covers both integer and float columns).

SQL `NULL` maps to Python `None` in both directions.

---

## 4. Connection Object

### Required methods

| Method | Description |
|---|---|
| `close()` | Close immediately.  All subsequent operations must raise `Error`. |
| `commit()` | Commit the current transaction.  For auto-commit databases, this should be a no-op. |
| `rollback()` | Roll back the current transaction.  May raise `NotSupportedError` if the database has no transaction support. |
| `cursor()` | Return a new `Cursor` object for the connection. |

### Notes

- Calling `close()` without first calling `commit()` must cause an implicit rollback.
- The connection object must not raise on repeated `close()` calls (implementation choice — the spec says nothing explicit, but convention is to be idempotent).
- PEP 249 says auto-commit **must be initially off**, but for databases like ClickHouse that have no transaction concept, `commit()` should succeed silently (no-op) rather than raising `NotSupportedError`.

### Optional extensions (commonly used by SQLAlchemy)

| Attribute/Method | Description |
|---|---|
| `autocommit` | Boolean read/write property |
| `Error`, `DatabaseError`, etc. | Exception classes re-exposed on the connection object |

---

## 5. Cursor Object

### Required attributes

| Attribute | R/W | Description |
|---|---|---|
| `description` | R | `None` or a sequence of 7-item sequences (see below). |
| `rowcount` | R | `-1` if unknown, otherwise count of rows produced/affected. |
| `arraysize` | R/W | Number of rows fetched by `fetchmany()` per call (default: `1`). |

### `description` format

After a SELECT (or other row-returning operation), `description` must be a sequence where each item is a 7-element sequence:

```
(name, type_code, display_size, internal_size, precision, scale, null_ok)
```

- `name` — column name (`str`), **mandatory**.
- `type_code` — one of the type singleton objects (`STRING`, `NUMBER`, etc.), **mandatory**.
- `display_size`, `internal_size`, `precision`, `scale`, `null_ok` — may be `None` if not available.

`description` must be `None`:
- before any `execute*()` call, and
- after operations that do not return rows (INSERT, DDL, etc.).

### `rowcount` semantics

| After operation | Expected value |
|---|---|
| `execute()` SELECT | Number of rows returned |
| `execute()` INSERT / UPDATE / DELETE | Number of rows affected |
| `execute()` DDL | `0` or `-1` |
| Before first `execute()` | `-1` |
| After `executemany()` | Total rows affected, or `-1` if not determinable |

### Required methods

| Method | Signature | Description |
|---|---|---|
| `callproc` | `(procname[, parameters])` | Call a stored procedure.  May raise `NotSupportedError`. |
| `close` | `()` | Close the cursor.  Subsequent operations must raise `InterfaceError`. |
| `execute` | `(operation[, parameters])` | Prepare and execute an operation. |
| `executemany` | `(operation, seq_of_parameters)` | Execute with multiple parameter sets. |
| `fetchone` | `()` | Return next row or `None`. |
| `fetchmany` | `([size])` | Return up to `size` rows (defaults to `arraysize`). |
| `fetchall` | `()` | Return all remaining rows. |
| `nextset` | `()` | Advance to next result set; return `True` or `None` if none. |
| `setinputsizes` | `(sizes)` | Pre-allocate parameter memory (may be a no-op). |
| `setoutputsize` | `(size[, column])` | Set buffer size for large columns (may be a no-op). |

### Fetch exhaustion rules

- `fetchone()` returns `None` when no more rows are available.
- `fetchmany()` returns an empty sequence (`[]`) when exhausted.
- `fetchall()` returns an empty sequence when exhausted.
- Calling any fetch method before `execute()` must raise `ProgrammingError`.

### Optional extensions used by SQLAlchemy

| Attribute | Description |
|---|---|
| `lastrowid` | ROWID / auto-increment ID of the last inserted row, or `None`. |
| `rownumber` | 0-based current row position in result set, or `None`. |
| `connection` | Read-only reference to the parent `Connection` object. |

---

## 6. Parameter Binding

SQLAlchemy translates its internal parameter representation into the driver's native `paramstyle` at dialect level, so **any** of the five styles is acceptable.  The driver must be consistent — mixing styles within one driver is not allowed.

Common choices for ClickHouse drivers:
- `pyformat` (`%(name)s`) — used by `clickhouse-driver`
- `format` (`%s`) — most common for positional params

---

## 7. SQLAlchemy-Specific Requirements

SQLAlchemy builds a dialect layer on top of DB-API 2.0.  Beyond strict PEP 249 compliance, the following are needed for full SQLAlchemy support.

### 7.1 Transaction management (`commit` / `rollback` must not raise)

SQLAlchemy always calls `connection.commit()` and `connection.rollback()` as part of its connection lifecycle — even for databases that are auto-commit.  If these methods raise `NotSupportedError`, SQLAlchemy will propagate the exception.

**Requirement:** `commit()` should be a silent no-op.  `rollback()` should either be a no-op or raise `NotSupportedError` only when it can be caught gracefully by the dialect.

### 7.2 `cursor.lastrowid`

Not in base PEP 249, but used by SQLAlchemy's ORM for identity management after `INSERT`:

```python
cursor.execute("INSERT INTO t VALUES (...)")
new_id = cursor.lastrowid  # e.g., 42
```

ClickHouse does not natively return inserted row IDs; this attribute should return `None` for ClickHouse.

### 7.3 `cursor.description` type codes

SQLAlchemy's type-affinity system maps `type_code` values to its own `TypeEngine` objects.  The `type_code` in `cursor.description` must be one of the PEP 249 type singletons (`STRING`, `NUMBER`, `DATETIME`, `BINARY`, `ROWID`), not a raw driver-specific string like `"UInt64"`.

### 7.4 `cursor.rowcount` for DML

SQLAlchemy's ORM uses `rowcount` after UPDATE and DELETE to verify that the expected number of rows was affected (optimistic locking / row-existence checks).  A value of `-1` is tolerated but causes SQLAlchemy to skip the check.

### 7.5 Module-level globals

SQLAlchemy reads `apilevel`, `threadsafety`, and `paramstyle` to configure its dialect:
- `paramstyle` tells the dialect which placeholder format to use.
- `threadsafety` informs connection pool sharing behaviour.

---

## 8. Async DB-API Considerations

PEP 249 is a **synchronous** specification.  `asynch` uses `async/await` throughout — every method that PEP 249 defines as synchronous is implemented as a coroutine.

This creates a compliance gap by design: `asynch` is not a drop-in replacement for a synchronous DB-API 2.0 driver, but it implements the same logical interface asynchronously.

### Integration path with SQLAlchemy async

SQLAlchemy 1.4+ supports asyncio via `create_async_engine`.  The typical wiring for asynch:

```python
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("clickhouse+asynch://user:pass@host/db")
```

This requires a SQLAlchemy dialect that wraps `asynch` (e.g., `clickhouse-sqlalchemy` with its `asynch` backend).  The dialect handles translating SQLAlchemy's synchronous cursor calls into `await cursor.execute(...)`.

### Module globals and type objects

These are synchronous/static by nature and are unaffected by asyncio.  They must be present regardless of the async interface.

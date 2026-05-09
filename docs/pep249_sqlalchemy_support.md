# PEP 249 and SQLAlchemy Support in asynch

This document provides an overview of the complete PEP 249 (Python Database API Specification v2.0) compliance and SQLAlchemy support implemented in asynch.

## 🎯 Implementation Overview

As of version 0.4.0, asynch is **fully compliant** with PEP 249 and provides seamless SQLAlchemy integration. All 259 PEP 249 compliance tests and 30 SQLAlchemy interface tests pass successfully.

## 📋 PEP 249 Compliance

### Module Globals

```python
import asynch

# Required PEP 249 globals
assert asynch.apilevel == "2.0"
assert asynch.threadsafety == 1  # Module shareable, connections per-event-loop
assert asynch.paramstyle == "pyformat"  # %(name)s style parameters
```

### Connection Factory

```python
# Standard PEP 249 connect() function
conn = asynch.connect(
    host="localhost",
    port=9000,
    user="default",
    password="",
    database="default"
)
```

### Complete Exception Hierarchy

All standard PEP 249 exceptions are available at module level:

```python
# Base exceptions
asynch.Warning
asynch.Error

# Specialized exceptions
asynch.InterfaceError
asynch.DatabaseError
asynch.DataError
asynch.OperationalError
asynch.IntegrityError
asynch.InternalError
asynch.ProgrammingError
asynch.NotSupportedError
```

### Type System

Complete type objects and constructors for proper data mapping:

```python
# Type objects for cursor.description
asynch.STRING    # String types (String, FixedString, etc.)
asynch.BINARY    # Binary data
asynch.NUMBER    # Numeric types (Int*, UInt*, Float*, Decimal)
asynch.DATETIME  # Date/time types (Date, DateTime, DateTime64)
asynch.ROWID     # Row identifiers

# Type constructors
asynch.Date(2024, 1, 1)
asynch.Time(12, 30, 45)
asynch.Timestamp(2024, 1, 1, 12, 30, 45)
asynch.DateFromTicks(1704110445)
asynch.TimeFromTicks(1704110445)
asynch.TimestampFromTicks(1704110445)
asynch.Binary(b"data")
```

### Cursor Interface

Full cursor API with proper PEP 249 semantics:

```python
async with conn.cursor() as cursor:
    # Execute with parameters
    await cursor.execute("SELECT * FROM table WHERE id = %(id)s", {"id": 1})

    # PEP 249 properties
    assert cursor.rowcount >= 0
    assert cursor.lastrowid is None  # ClickHouse doesn't support this
    assert cursor.arraysize > 0  # Default fetch size

    # Description with type objects
    print(cursor.description)  # [(name, type_code, ...)]
    assert cursor.description[0][1] in (asynch.STRING, asynch.NUMBER, asynch.DATETIME)

    # Standard fetch methods
    row = await cursor.fetchone()
    rows = await cursor.fetchmany(10)
    all_rows = await cursor.fetchall()

    # Optional methods
    cursor.setoutputsize(1000)  # No-op for ClickHouse
    cursor.setinputsizes([])    # No-op for ClickHouse
```

### Connection Interface

Standard connection lifecycle management:

```python
async with conn:
    # Transactions (no-op for ClickHouse but PEP 249 compliant)
    await conn.commit()    # Does not raise
    await conn.rollback()  # Does not raise

    # Cursor factory
    cursor = conn.cursor()
```

## 🔗 SQLAlchemy Integration

### Core Support

```python
from sqlalchemy.ext.asyncio import create_async_engine
import sqlalchemy as sa

# Create engine
engine = create_async_engine("clickhouse+asynch://user:pass@host:port/db")

async with engine.begin() as conn:
    # Text queries
    result = await conn.execute(sa.text("SELECT version()"))

    # Core constructs
    metadata = sa.MetaData()
    table = sa.Table('users', metadata, autoload_with=conn)
    query = sa.select(table).where(table.c.id > 10)
    result = await conn.execute(query)
```

### ORM Support

When used with [clickhouse-sqlalchemy](https://github.com/xzkostyan/clickhouse-sqlalchemy):

```python
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

# Session management
async_session = async_sessionmaker(engine)

async with async_session() as session:
    users = await session.execute(sa.select(User))
    await session.commit()
```

## 🧪 Testing

Comprehensive test suites validate the implementation:

### PEP 249 Tests
```bash
make test-pep249  # 259 tests covering every PEP 249 requirement
```

### SQLAlchemy Tests
```bash
make test-sqlalchemy  # 49 tests (30 interface + 19 integration tests)
```

### Combined Testing
```bash
make test-compat  # Run both test suites together
```

## 📖 Documentation References

- **[PEP 249 Requirements](pep249_requirements.md)**: Complete specification reference
- **[PEP 249 Implementation Plan](pep249_implementation_plan.md)**: Gap analysis and implementation details
- **[Local Development Setup](local_clickhouse_development.md)**: Development environment setup

## 💡 Migration Guide

If you're upgrading from pre-0.4.0 versions, your existing code continues to work unchanged. The new PEP 249 interface provides additional ways to use asynch:

### Before (still works):
```python
from asynch import Connection

async with Connection(host="localhost") as conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
```

### Now also available:
```python
import asynch

# PEP 249 style
conn = asynch.connect(host="localhost")
async with conn:
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")

# With exception handling
try:
    async with conn.cursor() as cursor:
        await cursor.execute("invalid sql")
except asynch.ProgrammingError:
    print("SQL syntax error")
```

This dual interface ensures backward compatibility while enabling modern Python database application patterns.
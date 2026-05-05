"""
Fixtures for SQLAlchemy compatibility tests.

These tests verify that asynch exposes the interface SQLAlchemy requires.
Some tests use SQLAlchemy directly; those are skipped if sqlalchemy is not installed.
Tests requiring a full ClickHouse-SQLAlchemy dialect (clickhouse-sqlalchemy)
are skipped if that package is not installed.

Connection settings are inherited from the root conftest via the `config` fixture.
"""

import pytest

# ---------------------------------------------------------------------------
# Optional-import guards
# ---------------------------------------------------------------------------

sqlalchemy = pytest.importorskip(
    "sqlalchemy",
    reason="sqlalchemy is not installed — skipping SQLAlchemy tests",
    allow_module_level=True,
)

# ---------------------------------------------------------------------------
# Table setup
# ---------------------------------------------------------------------------

SA_TABLE = "test.sa_compat"
SA_DDL = f"""
    CREATE TABLE IF NOT EXISTS {SA_TABLE}
    (
        id       Int32,
        name     Nullable(String),
        score    Float64,
        created  Date
    )
    ENGINE = MergeTree
    ORDER BY id
"""


@pytest.fixture(scope="session", autouse=True)
async def sa_table(config):
    """Create the SQLAlchemy compatibility test table once per session."""
    from asynch.connection import Connection

    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("CREATE DATABASE IF NOT EXISTS test")
            await cursor.execute(f"DROP TABLE IF EXISTS {SA_TABLE}")
            await cursor.execute(SA_DDL)
    yield
    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"DROP TABLE IF EXISTS {SA_TABLE}")


@pytest.fixture(autouse=True)
async def truncate_sa(config):
    """Truncate the SA compat table before each test."""
    from asynch.connection import Connection

    async with Connection(dsn=config.dsn) as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"TRUNCATE TABLE {SA_TABLE}")
    yield


@pytest.fixture
async def sa_conn(config):
    """Raw asynch connection for tests that work at DB-API level."""
    from asynch.connection import Connection

    async with Connection(dsn=config.dsn) as conn:
        yield conn


@pytest.fixture
async def sa_cursor(sa_conn):
    """Raw asynch cursor."""
    async with sa_conn.cursor() as cursor:
        yield cursor


# ---------------------------------------------------------------------------
# SQLAlchemy async engine fixture (requires clickhouse-sqlalchemy)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ch_sa_dialect():
    """Import marker: skip if clickhouse-sqlalchemy is not installed."""
    try:
        import clickhouse_sqlalchemy  # noqa: F401

        return clickhouse_sqlalchemy
    except ImportError:
        pytest.skip("clickhouse-sqlalchemy is not installed")


@pytest.fixture(scope="session")
def async_engine(config, ch_sa_dialect):
    """Create a SQLAlchemy async engine backed by asynch."""
    from sqlalchemy.ext.asyncio import create_async_engine

    url = (
        f"clickhouse+asynch://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )
    engine = create_async_engine(url, echo=False)
    yield engine


@pytest.fixture
async def async_session(async_engine):
    """Provide an AsyncSession for ORM tests."""
    from sqlalchemy.ext.asyncio import AsyncSession

    async with AsyncSession(async_engine, expire_on_commit=False) as session:
        yield session

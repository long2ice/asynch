"""
SQLAlchemy Core compatibility tests.

These tests require both sqlalchemy and clickhouse-sqlalchemy to be installed.
If clickhouse-sqlalchemy is not available, all tests in this module are skipped.

The tests verify that asynch can be used as the underlying driver for
SQLAlchemy Core operations (DDL, DML, queries) via the clickhouse+asynch dialect.

Install requirements:
    pip install sqlalchemy clickhouse-sqlalchemy
"""
import datetime

import pytest

# All tests in this module require clickhouse-sqlalchemy
pytestmark = pytest.mark.usefixtures("ch_sa_dialect")

SA_TABLE = "test.sa_compat"


class TestEngineCreation:
    def test_async_engine_created(self, async_engine):
        assert async_engine is not None

    async def test_engine_connect(self, async_engine):
        from sqlalchemy.ext.asyncio import AsyncConnection

        async with async_engine.connect() as conn:
            assert isinstance(conn, AsyncConnection)


class TestCoreTextQueries:
    """SQLAlchemy Core text() queries via asynch."""

    async def test_select_constant(self, async_engine):
        from sqlalchemy import text

        async with async_engine.connect() as conn:
            result = await conn.execute(text("SELECT 1 AS n"))
            row = result.fetchone()
            assert row is not None
            assert row[0] == 1

    async def test_select_with_params(self, async_engine):
        from sqlalchemy import text

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT :val AS n"),
                {"val": 42},
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == 42

    async def test_insert_via_text(self, async_engine):
        from sqlalchemy import text

        async with async_engine.connect() as conn:
            await conn.execute(
                text(
                    f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES "
                    "(:id, :name, :score, :created)"
                ),
                {"id": 1, "name": "Alice", "score": 9.5, "created": datetime.date(2024, 1, 1)},
            )
            await conn.commit()

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT name FROM {SA_TABLE} WHERE id = 1")
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "Alice"


class TestCoreDDL:
    """CREATE / DROP TABLE via SQLAlchemy Core metadata."""

    async def test_create_and_drop_table(self, async_engine):
        from sqlalchemy import Column, Integer, MetaData, String, Table

        metadata = MetaData()
        test_table = Table(
            "sa_core_ddl_test",
            metadata,
            Column("id", Integer),
            Column("name", String),
            schema="test",
        )

        async with async_engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
            await conn.run_sync(metadata.drop_all)


class TestCoreResultMapping:
    """Verify that result rows map column names correctly."""

    async def test_column_names_accessible(self, async_engine):
        from sqlalchemy import text

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text("SELECT toInt32(1) AS id, 'hello' AS name")
            )
            row = result.mappings().fetchone()
            assert row is not None
            assert row["id"] == 1
            assert row["name"] == "hello"

    async def test_fetchall_returns_rows(self, async_engine):
        from sqlalchemy import text

        # Insert several rows first
        async with async_engine.connect() as conn:
            for i in range(1, 4):
                await conn.execute(
                    text(
                        f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES "
                        "(:id, :name, :score, :created)"
                    ),
                    {"id": i, "name": f"row{i}", "score": float(i),
                     "created": datetime.date(2024, 1, 1)},
                )
            await conn.commit()

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT id, name FROM {SA_TABLE} ORDER BY id")
            )
            rows = result.fetchall()
            assert len(rows) == 3
            assert rows[0][0] == 1


class TestCoreTransaction:
    """SQLAlchemy Core transaction management with asynch."""

    async def test_autobegin_and_commit(self, async_engine):
        from sqlalchemy import text

        async with async_engine.begin() as conn:
            await conn.execute(
                text(
                    f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES "
                    "(:id, :name, :score, :created)"
                ),
                {"id": 10, "name": "tx_test", "score": 1.0,
                 "created": datetime.date(2024, 1, 1)},
            )
            # commit happens automatically on context manager exit

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text(f"SELECT name FROM {SA_TABLE} WHERE id = 10")
            )
            row = result.fetchone()
            assert row is not None
            assert row[0] == "tx_test"

    async def test_rollback_on_exception(self, async_engine):
        from sqlalchemy import text

        try:
            async with async_engine.begin() as conn:
                await conn.execute(
                    text(
                        f"INSERT INTO {SA_TABLE} (id, name, score, created) VALUES "
                        "(:id, :name, :score, :created)"
                    ),
                    {"id": 20, "name": "should_rollback", "score": 0.0,
                     "created": datetime.date(2024, 1, 1)},
                )
                raise ValueError("intentional error to trigger rollback")
        except ValueError:
            pass

        # ClickHouse is auto-commit — the row may or may not be visible.
        # This test verifies no exception propagation from the rollback itself.

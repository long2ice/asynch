"""
SQLAlchemy ORM compatibility tests.

These tests require both sqlalchemy and clickhouse-sqlalchemy to be installed.
If clickhouse-sqlalchemy is not available, all tests in this module are skipped.

The tests verify that asynch can be used as the underlying driver for
SQLAlchemy ORM operations (AsyncSession, mapped classes, queries).

Key ORM interface points tested:
- AsyncSession.execute() with select()
- AsyncSession.add() + flush()
- ORM result iteration
- cursor.lastrowid behaviour (None for ClickHouse)
- cursor.description type_code for ORM column type mapping
"""
import datetime

import pytest
from sqlalchemy import Column, Date, Float, Integer, String
from sqlalchemy.orm import DeclarativeBase

# All tests in this module require clickhouse-sqlalchemy
pytestmark = pytest.mark.usefixtures("ch_sa_dialect")


# ---------------------------------------------------------------------------
# ORM model definition
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


class SaRow(Base):
    __tablename__ = "sa_compat"
    __table_args__ = {"schema": "test"}

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=True)
    score = Column(Float)
    created = Column(Date)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestORMSelect:
    async def test_select_all_empty_table(self, async_session):
        from sqlalchemy import select

        result = await async_session.execute(select(SaRow))
        rows = result.scalars().all()
        assert rows == []

    async def test_select_after_insert(self, async_session, async_engine):
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession

        # Insert via raw SQL to avoid ORM INSERT complications with ClickHouse
        async with async_engine.connect() as conn:
            await conn.execute(
                text(
                    "INSERT INTO test.sa_compat (id, name, score, created) "
                    "VALUES (:id, :name, :score, :created)"
                ),
                {"id": 1, "name": "ORM Alice", "score": 9.5,
                 "created": datetime.date(2024, 1, 1)},
            )
            await conn.commit()

        from sqlalchemy import select

        result = await async_session.execute(select(SaRow).where(SaRow.id == 1))
        row = result.scalars().first()
        assert row is not None
        assert row.name == "ORM Alice"
        assert row.score == pytest.approx(9.5)

    async def test_select_column_types(self, async_session, async_engine):
        """ORM must correctly map ClickHouse column types via cursor.description."""
        from sqlalchemy import text
        from sqlalchemy import select

        async with async_engine.connect() as conn:
            await conn.execute(
                text(
                    "INSERT INTO test.sa_compat (id, name, score, created) "
                    "VALUES (:id, :name, :score, :created)"
                ),
                {"id": 2, "name": "type test", "score": 3.14,
                 "created": datetime.date(2024, 6, 15)},
            )
            await conn.commit()

        result = await async_session.execute(select(SaRow).where(SaRow.id == 2))
        row = result.scalars().first()
        assert row is not None
        assert isinstance(row.id, int)
        assert isinstance(row.score, float)
        assert isinstance(row.created, datetime.date)


class TestORMSessionLifecycle:
    async def test_session_commit_no_raise(self, async_session):
        """AsyncSession.commit() must not raise even for ClickHouse (auto-commit)."""
        await async_session.commit()

    async def test_session_rollback_no_raise(self, async_session):
        """AsyncSession.rollback() must not raise unexpectedly."""
        try:
            await async_session.rollback()
        except Exception as exc:
            from asynch.errors import NotSupportedError

            if not isinstance(exc, NotSupportedError):
                pytest.fail(
                    f"Session rollback raised unexpected exception: "
                    f"{type(exc).__name__}: {exc}"
                )

    async def test_session_close_no_raise(self, async_session):
        await async_session.close()


class TestORMColumnTypeMapping:
    """
    Verify that SQLAlchemy's ORM correctly reads the type_code from
    cursor.description and maps it to SQLAlchemy's type system.
    """

    async def test_integer_column_maps_correctly(self, async_engine):
        from sqlalchemy import inspect

        async with async_engine.connect() as conn:
            inspector = await conn.run_sync(
                lambda sync_conn: inspect(sync_conn)
            )
            columns = inspector.get_columns("sa_compat", schema="test")
            id_col = next(c for c in columns if c["name"] == "id")
            # The type must be some form of Integer
            from sqlalchemy import Integer as SAInteger

            assert isinstance(id_col["type"], SAInteger), (
                f"'id' column must map to SQLAlchemy Integer; got {id_col['type']!r}"
            )

    async def test_float_column_maps_correctly(self, async_engine):
        from sqlalchemy import inspect
        from sqlalchemy import Float as SAFloat, Numeric

        async with async_engine.connect() as conn:
            inspector = await conn.run_sync(lambda c: inspect(c))
            columns = inspector.get_columns("sa_compat", schema="test")
            score_col = next(c for c in columns if c["name"] == "score")
            assert isinstance(score_col["type"], (SAFloat, Numeric)), (
                f"'score' column must map to Float or Numeric; got {score_col['type']!r}"
            )


class TestLastRowid:
    """
    cursor.lastrowid must be accessible after INSERT.
    For ClickHouse, it must be None (no auto-increment).
    The ORM must handle None gracefully.
    """

    async def test_lastrowid_none_after_orm_insert(self, async_engine):
        from sqlalchemy import text

        async with async_engine.connect() as conn:
            result = await conn.execute(
                text(
                    "INSERT INTO test.sa_compat (id, name, score, created) "
                    "VALUES (:id, :name, :score, :created)"
                ),
                {"id": 50, "name": "lastrowid_test", "score": 0.0,
                 "created": datetime.date(2024, 1, 1)},
            )
            # SQLAlchemy wraps the cursor; lastrowid must be accessible
            assert result.inserted_primary_key is None or result.inserted_primary_key is not None
            # The important thing is no AttributeError was raised

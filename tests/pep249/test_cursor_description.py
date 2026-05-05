"""
PEP 249 — cursor.description compliance tests.

Covers:
- description is None before execute()
- description is None after non-row-returning operations (INSERT, DDL)
- description is a sequence of 7-item sequences after SELECT
- Each item: (name, type_code, display_size, internal_size, precision, scale, null_ok)
- name must be a string (mandatory)
- type_code must be one of the PEP 249 type objects (mandatory)
- Remaining fields may be None
- type_code correctly maps ClickHouse types to PEP 249 type objects
"""
import datetime

import pytest

import asynch
from asynch.cursors import Column

PEP249_TABLE = "test.pep249"


class TestDescriptionBeforeExecute:
    def test_none_before_execute(self, pep249_conn):
        cursor = pep249_conn.cursor()
        assert cursor.description is None, "description must be None before execute()"


class TestDescriptionAfterNonSelect:
    async def test_none_after_insert(self, pep249_cursor):
        await pep249_cursor.execute(
            f"INSERT INTO {PEP249_TABLE} (id, name, value, created, flag) VALUES",
            [(99, "x", 0.0, datetime.date(2024, 1, 1), True)],
        )
        assert pep249_cursor.description is None, (
            "description must be None after INSERT (no result set returned)"
        )

    async def test_none_after_ddl(self, pep249_cursor):
        await pep249_cursor.execute(
            "CREATE TABLE IF NOT EXISTS test.pep249_desc_ddl_test "
            "(id Int32) ENGINE = MergeTree ORDER BY id"
        )
        assert pep249_cursor.description is None, (
            "description must be None after DDL (no result set returned)"
        )
        await pep249_cursor.execute("DROP TABLE IF EXISTS test.pep249_desc_ddl_test")


class TestDescriptionStructure:
    """After a SELECT, description must be a sequence of 7-item sequences."""

    async def test_description_is_sequence(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        assert pep249_cursor.description is not None
        assert hasattr(pep249_cursor.description, "__iter__"), (
            "description must be iterable"
        )

    async def test_description_has_one_item_per_column(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS a, 2 AS b, 3 AS c")
        assert len(pep249_cursor.description) == 3

    async def test_each_item_is_seven_elements(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        for item in pep249_cursor.description:
            assert len(item) == 7, (
                f"Each description item must have 7 elements; got {len(item)}"
            )

    async def test_item_is_indexable(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        item = pep249_cursor.description[0]
        _ = item[0]  # name
        _ = item[1]  # type_code

    async def test_description_unpacks_as_seven_tuple(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        name, type_code, display_size, internal_size, precision, scale, null_ok = (
            pep249_cursor.description[0]
        )


class TestDescriptionName:
    """The first element (name) must be a string."""

    async def test_name_is_string(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS my_column")
        name = pep249_cursor.description[0][0]
        assert isinstance(name, str), f"description name must be str; got {type(name)}"

    async def test_name_matches_column_alias(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS my_alias")
        name = pep249_cursor.description[0][0]
        assert name == "my_alias"

    async def test_multiple_column_names(self, pep249_cursor, populated_table):
        await pep249_cursor.execute(
            f"SELECT id, name, value, created, flag FROM {PEP249_TABLE} LIMIT 1"
        )
        names = [item[0] for item in pep249_cursor.description]
        assert names == ["id", "name", "value", "created", "flag"]


class TestDescriptionTypeCode:
    """The second element (type_code) must be a PEP 249 type singleton."""

    async def test_type_code_is_not_none(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        type_code = pep249_cursor.description[0][1]
        assert type_code is not None, "type_code must not be None"

    async def test_type_code_is_not_raw_string(self, pep249_cursor):
        """type_code must be a PEP 249 type object, not a raw ClickHouse type string."""
        await pep249_cursor.execute("SELECT 1 AS n")
        type_code = pep249_cursor.description[0][1]
        assert not isinstance(type_code, str), (
            f"type_code must be a PEP 249 type object, not a string; got {type_code!r}"
        )

    async def test_type_code_equals_number_for_int(self, pep249_cursor):
        await pep249_cursor.execute("SELECT toInt32(1) AS n")
        type_code = pep249_cursor.description[0][1]
        assert type_code == asynch.NUMBER, (
            f"Int32 column type_code must equal asynch.NUMBER; got {type_code!r}"
        )

    async def test_type_code_equals_string_for_string(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 'hello' AS s")
        type_code = pep249_cursor.description[0][1]
        assert type_code == asynch.STRING, (
            f"String column type_code must equal asynch.STRING; got {type_code!r}"
        )

    async def test_type_code_equals_datetime_for_date(self, pep249_cursor, populated_table):
        await pep249_cursor.execute(f"SELECT created FROM {PEP249_TABLE} LIMIT 1")
        type_code = pep249_cursor.description[0][1]
        assert type_code == asynch.DATETIME, (
            f"Date column type_code must equal asynch.DATETIME; got {type_code!r}"
        )

    async def test_type_code_equals_number_for_float(self, pep249_cursor, populated_table):
        await pep249_cursor.execute(f"SELECT value FROM {PEP249_TABLE} LIMIT 1")
        type_code = pep249_cursor.description[0][1]
        assert type_code == asynch.NUMBER, (
            f"Float64 column type_code must equal asynch.NUMBER; got {type_code!r}"
        )

    async def test_type_code_in_table_columns(self, pep249_cursor, populated_table):
        """All columns from the pep249 table must have valid PEP 249 type codes."""
        await pep249_cursor.execute(
            f"SELECT id, name, value, created, flag FROM {PEP249_TABLE} LIMIT 1"
        )
        valid_types = {asynch.STRING, asynch.NUMBER, asynch.DATETIME, asynch.BINARY, asynch.ROWID}
        for item in pep249_cursor.description:
            type_code = item[1]
            is_valid = any(type_code == t for t in valid_types)
            assert is_valid, (
                f"Column '{item[0]}' has type_code {type_code!r} which is not a "
                f"recognised PEP 249 type object"
            )


class TestDescriptionOptionalFields:
    """Fields 3–7 (display_size through null_ok) may be None per spec."""

    async def test_optional_fields_are_none_or_value(self, pep249_cursor):
        await pep249_cursor.execute("SELECT 1 AS n")
        _, _, display_size, internal_size, precision, scale, null_ok = (
            pep249_cursor.description[0]
        )
        # Each field is either None or a valid value — no hard type requirement
        for field in (display_size, internal_size, precision, scale):
            assert field is None or isinstance(field, int), (
                f"Optional numeric description field must be None or int; got {field!r}"
            )
        assert null_ok is None or isinstance(null_ok, bool), (
            f"null_ok must be None or bool; got {null_ok!r}"
        )

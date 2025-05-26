import ssl
from contextlib import nullcontext as does_not_raise
from datetime import date, datetime
from enum import Enum
from typing import Any, ContextManager, Optional
from uuid import UUID

import pytest
from pytest import param

from asynch.proto.utils.escape import escape_param, escape_params, substitute_params


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        param(None, "NULL", id="None"),
        param(date(2025, 5, 21), "'2025-05-21'", id="date"),
        param(datetime(2025, 5, 21, 12, 0, 0), "'2025-05-21 12:00:00'", id="datetime"),
        param("test", "'test'", id="str"),
        param([1, 2, 3], "[1, 2, 3]", id="list"),
        param((1, 2, 3), "(1, 2, 3)", id="tuple"),
        param(
            UUID("123e4567-e89b-12d3-a456-426614174000"),
            "'123e4567-e89b-12d3-a456-426614174000'",
            id="uuid",
        ),
        param(Enum("testEnum", {"test1": "test1", "test2": "test2"}).test1, "'test1'", id="enum"),
    ],
)
def test_escape_param(item, expected):
    assert escape_param(item) == expected


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        param({"test": "test"}, {"test": "'test'"}, id="strings"),
        param(
            {"test": 1, "test2": 2, "test3": 0, "test4": -1},
            {"test": 1, "test2": 2, "test3": 0, "test4": -1},
            id="ints",
        ),
        param(
            {"test": 1.0, "test2": 0.1, "test3": -0.1, "test4": 0.0},
            {"test": 1.0, "test2": 0.1, "test3": -0.1, "test4": 0.0},
            id="floats",
        ),
        param({"test": None}, {"test": "NULL"}, id="none"),
        param({"test": date(2025, 5, 21)}, {"test": "'2025-05-21'"}, id="dates"),
        param(
            {"test": datetime(2025, 5, 21, 12, 0, 0)},
            {"test": "'2025-05-21 12:00:00'"},
            id="datetimes",
        ),
        param(
            {"test": UUID("123e4567-e89b-12d3-a456-426614174000")},
            {"test": "'123e4567-e89b-12d3-a456-426614174000'"},
            id="uuids",
        ),
    ],
)
def test_escape_params(params, expected):
    assert escape_params(params) == expected


@pytest.mark.parametrize(
    ("query", "params", "expected"),
    [
        param("{test}", {"test": "test"}, "'test'", id="string"),
        param("{test} {test}", {"test": "test"}, "'test' 'test'", id="strings"),
        param("{test} {test}", {"test": None}, "NULL NULL", id="none"),
        param("{test} {test}", {"test": 1}, "1 1", id="int"),
        param("{test} {test}", {"test": 1.0}, "1.0 1.0", id="float"),
        param("{test} {test}", {"test": date(2025, 5, 21)}, "'2025-05-21' '2025-05-21'", id="date"),
        param(
            "{test} {test}",
            {"test": datetime(2025, 5, 21, 12, 0, 0)},
            "'2025-05-21 12:00:00' '2025-05-21 12:00:00'",
            id="datetime",
        ),
        param(
            "{test} {test}",
            {"test": UUID("123e4567-e89b-12d3-a456-426614174000")},
            "'123e4567-e89b-12d3-a456-426614174000' '123e4567-e89b-12d3-a456-426614174000'",
        ),
    ],
)
def test_substitute_params(query, params, expected):
    assert substitute_params(query, params) == expected

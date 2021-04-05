import pytest

from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.stringcolumn import FixedString, String


@pytest.mark.parametrize(
    "spec, expected_column", [("String", String), ("FixedString(10)", FixedString)]
)
def test_create_tuple_column(spec, column_options, expected_column):
    column = get_column_by_spec(spec, column_options)

    assert isinstance(column, expected_column)

import pytest

from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.stringcolumn import FixedString, String


@pytest.mark.parametrize(
    "spec, expected_column", [("String", String), ("FixedString(10)", FixedString)]
)
def test_create_string_column(spec, column_options, expected_column):
    column = get_column_by_spec(spec, column_options)

    assert isinstance(column, expected_column)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec, items, expected_buffer", [
        ("String", ['1'], bytearray(b'\x011')),
        ("FixedString(2)", ['12', '34'], bytearray(b'1234'))
    ]
)
async def test_write_data_items(spec, items, expected_buffer, column_options):
    column = get_column_by_spec(spec, column_options)
    await column.write_items(items)

    assert column.writer.buffer == expected_buffer

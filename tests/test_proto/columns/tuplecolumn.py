from asyncio.streams import StreamReader

import pytest

from asynch.proto import constants
from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.tuplecolumn import TupleColumn
from asynch.proto.context import Context
from asynch.proto.io import BufferedReader, BufferedWriter

COLUMN_SPEC = "Tuple(UInt8, String)"


@pytest.fixture
def tuple_column():
    reader = BufferedReader(StreamReader(), constants.BUFFER_SIZE)
    writer = BufferedWriter()
    context = Context()
    context.client_settings = {
        "strings_as_bytes": False,
        "strings_encoding": constants.STRINGS_ENCODING,
    }
    column_options = {"reader": reader, "writer": writer, "context": context}
    column = get_column_by_spec(COLUMN_SPEC, column_options)
    return column


def test_create_tuple_column(tuple_column):
    assert isinstance(tuple_column, TupleColumn)
    assert len(tuple_column.nested_columns) == 2


@pytest.mark.asyncio
async def test_TupleColumn_write_data_empty_items(tuple_column):
    await tuple_column.write_items(())

    assert len(tuple_column.writer.buffer) == 0


@pytest.mark.asyncio
async def test_TupleColumn_write_data_items(tuple_column):
    await tuple_column.write_items([(0, "a")])

    assert len(tuple_column.writer.buffer) == 3

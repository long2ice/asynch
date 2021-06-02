import pytest

from asynch.proto.columns import get_column_by_spec
from asynch.proto.columns.lowcardinalitycolumn import LowCardinalityColumn
from asynch.proto.columns.stringcolumn import String

COLUMN_SPEC = "LowCardinality(String)"


@pytest.fixture
def low_cardinality_column(column_options):
    column = get_column_by_spec(COLUMN_SPEC, column_options)
    return column


def test_create_lc_column(low_cardinality_column):
    assert isinstance(low_cardinality_column, LowCardinalityColumn)
    assert isinstance(low_cardinality_column.nested_column, String)


@pytest.mark.asyncio
async def test_lc_column_write_data(low_cardinality_column):
    await low_cardinality_column.write_data('')
    assert len(low_cardinality_column.writer.buffer) == 0

    await low_cardinality_column.write_data('1234567890')
    assert len(low_cardinality_column.writer.buffer) == 54


@pytest.mark.asyncio
async def test_lc_column_read_data(low_cardinality_column, mocker):
    s = '1234567890'
    await low_cardinality_column.write_data(s)
    mocker.patch.object(
        low_cardinality_column.reader.reader, 'read', return_value=low_cardinality_column.writer.buffer
    )
    resp = await low_cardinality_column.read_data(len(s))
    assert resp == tuple(s)

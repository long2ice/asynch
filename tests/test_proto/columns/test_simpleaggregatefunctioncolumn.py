import pytest
from asynch.proto.columns import get_column_by_spec

COLUMN_SPEC = (
    "SimpleAggregateFunction("
    "groupArrayArray, Array(Tuple(UInt8, String))"
    ")"
)


@pytest.fixture
def simple_aggregate_function_column(column_options):
    column = get_column_by_spec(COLUMN_SPEC, column_options)
    return column


def test_create_simple_aggregate_function_column(simple_aggregate_function_column):
    nested_column = simple_aggregate_function_column.nested_column
    assert len(nested_column.nested_columns) == 2


@pytest.mark.asyncio
async def test_SimpleAggregateFunctionColumn_write_data_empty(simple_aggregate_function_column):
    await simple_aggregate_function_column.write_data(())

    assert len(simple_aggregate_function_column.writer.buffer) == 0


@pytest.mark.asyncio
async def test_SimpleAggregateFunctionColumn_write_data(simple_aggregate_function_column):
    await simple_aggregate_function_column.write_data([[(0, "a"), (1, "b")]])

    assert len(simple_aggregate_function_column.writer.buffer) == 14

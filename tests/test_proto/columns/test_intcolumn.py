import pytest

from asynch.proto.columns import get_column_by_spec


@pytest.mark.asyncio
async def test_Int64Column_write_data(column_options):
    column = get_column_by_spec("Int64", column_options)
    await column.write_items(([-42]))

    assert len(column.writer.buffer) == 8


@pytest.mark.asyncio
async def test_UInt64Column_write_data(column_options):
    column = get_column_by_spec("UInt64", column_options)
    await column.write_items(([42]))

    assert len(column.writer.buffer) == 8


@pytest.mark.asyncio
async def test_Int128Column_write_data(column_options):
    column = get_column_by_spec("Int128", column_options)
    await column.write_items(([-42]))

    assert len(column.writer.buffer) == 16


@pytest.mark.asyncio
async def test_UInt128Column_write_data(column_options):
    column = get_column_by_spec("UInt128", column_options)
    await column.write_items(([42]))

    assert len(column.writer.buffer) == 16

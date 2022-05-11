from asynch.proto.utils.helpers import pairwise

from .base import Column
from .intcolumn import UInt64Column


class MapColumn(Column):
    py_types = (dict,)

    def __init__(self, key_column, value_column, **kwargs):
        kwargs.update(dict(reader=key_column.reader, writer=key_column.writer))
        self.offset_column = UInt64Column(**kwargs)
        self.key_column = key_column
        self.value_column = value_column
        super(MapColumn, self).__init__(**kwargs)

    async def read_state_prefix(self):
        await self.key_column.read_state_prefix()
        await self.value_column.read_state_prefix()

    async def write_state_prefix(self):
        await self.key_column.write_state_prefix()
        await self.value_column.write_state_prefix()

    async def read_items(self, n_items):
        offsets = list(await self.offset_column.read_items(n_items))
        last_offset = offsets[-1]
        keys = await self.key_column.read_data(last_offset)
        values = await self.value_column.read_data(last_offset)

        offsets.insert(0, 0)

        return [dict(zip(keys[begin:end], values[begin:end])) for begin, end in pairwise(offsets)]

    async def write_items(self, items):
        offsets = []
        keys = []
        values = []

        total = 0
        for x in items:
            total += len(x)
            offsets.append(total)
            keys.extend(x.keys())
            values.extend(x.values())

        await self.offset_column.write_items(offsets)
        await self.key_column.write_data(keys)
        await self.value_column.write_data(values)


def create_map_column(spec, column_by_spec_getter):
    key, value = spec[4:-1].split(",")
    key_column = column_by_spec_getter(key.strip())
    value_column = column_by_spec_getter(value.strip())

    return MapColumn(key_column, value_column)

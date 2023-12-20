import json

from .base import Column
from .stringcolumn import String


class JsonColumn(Column):
    py_types = (dict,)

    # No NULL value actually
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super(JsonColumn, self).__init__(**kwargs)

    async def write_state_prefix(self):
        await self.writer.write_uint8(1)

    async def read_items(self, n_items):
        await self.reader.read_uint8()
        spec = await self.reader.read_str()
        col = self.column_by_spec_getter(spec)
        await col.read_state_prefix()
        return await col.read_data(n_items)

    async def write_items(self, items):
        items = [x if isinstance(x, str) else json.dumps(x) for x in items]
        await self.string_column.write_items(items)


def create_json_column(spec, column_by_spec_getter, column_options):
    return JsonColumn(column_by_spec_getter, **column_options)

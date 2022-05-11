from uuid import UUID

from ...errors import CannotParseUuidError
from ..streams.buffered import MAX_UINT64
from ..utils import compat
from .base import FormatColumn


class UUIDColumn(FormatColumn):
    ch_type = "UUID"
    py_types = compat.string_types + (UUID,)
    format = "Q"

    # UUID is stored by two uint64 numbers.
    async def write_items(
        self,
        items,
    ):
        n_items = len(items)

        uint_64_pairs = [None] * 2 * n_items
        for i, x in enumerate(items):
            i2 = 2 * i
            uint_64_pairs[i2] = (x >> 64) & MAX_UINT64
            uint_64_pairs[i2 + 1] = x & MAX_UINT64

        s = self.make_struct(2 * n_items)
        await self.writer.write_bytes(s.pack(*uint_64_pairs))

    async def read_items(
        self,
        n_items,
    ):
        # TODO: cythonize
        s = self.make_struct(2 * n_items)
        items = s.unpack(await self.reader.read_bytes(s.size))

        uint_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            uint_128_items[i] = (items[i2] << 64) + items[i2 + 1]

        return tuple(uint_128_items)

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(UUID(int=item) for item in items)
        else:
            return tuple(
                (None if is_null else UUID(int=items[i])) for i, is_null in enumerate(nulls_map)
            )

    def before_write_items(self, items, nulls_map=None):
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            try:
                if not isinstance(item, UUID):
                    item = UUID(item)

            except ValueError:
                raise CannotParseUuidError("Cannot parse uuid '{}'".format(item))

            items[i] = item.int

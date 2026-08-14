# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from uuid import UUID, SafeUUID

from ...errors import CannotParseUuidError
from ..streams.buffered import MAX_UINT64
from .base import FormatColumn


_UUID_NEW = UUID.__new__
_OBJ_SETATTR = object.__setattr__
_SAFE_UNKNOWN = SafeUUID.unknown


def _make_uuid(int_value):
    """Build a UUID without UUID.__init__'s argument dispatch and validation.

    Equivalent to UUID(int=value) for the 0 <= value < 2**128 ints produced by
    the wire format; `int`/`is_safe` are UUID's documented __slots__.
    """
    u = _UUID_NEW(UUID)
    _OBJ_SETATTR(u, "int", int_value)
    _OBJ_SETATTR(u, "is_safe", _SAFE_UNKNOWN)
    return u


class UUIDColumn(FormatColumn):
    ch_type = "UUID"
    py_types = (str, UUID)
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
        s = self.make_struct(2 * n_items)
        items = s.unpack(await self.reader.read_bytes_view(s.size))

        uint_128_items = [None] * n_items
        for i in range(n_items):
            i2 = 2 * i
            uint_128_items[i] = (items[i2] << 64) + items[i2 + 1]

        return tuple(uint_128_items)

    def after_read_items(self, items, nulls_map=None):
        if nulls_map is None:
            return tuple(_make_uuid(item) for item in items)
        else:
            return tuple(
                (None if is_null else _make_uuid(items[i])) for i, is_null in enumerate(nulls_map)
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
                raise CannotParseUuidError(f"Cannot parse uuid '{item}'")

            items[i] = item.int

# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from itertools import chain
from struct import Struct

from .base import Column
from .intcolumn import UInt64Column


class ArrayColumn(Column):
    """
    Nested arrays written in flatten form after information about their
    sizes (offsets really).
    One element of array of arrays can be represented as tree:
    (0 depth)          [[3, 4], [5, 6]]
                      |               |
    (1 depth)      [3, 4]           [5, 6]
                   |    |           |    |
    (leaf)        3     4          5     6

    Offsets (sizes) written in breadth-first search order. In example above
    following sequence of offset will be written: 4 -> 2 -> 4
    1) size of whole array: 4
    2) size of array 1 in depth=1: 2
    3) size of array 2 plus size of all array before in depth=1: 2 + 2 = 4

    After sizes info comes flatten data: 3 -> 4 -> 5 -> 6
    """

    py_types = (list, tuple)
    size_struct = Struct("<Q")

    def __init__(self, nested_column, **kwargs):
        kwargs.update(dict(reader=nested_column.reader, writer=nested_column.writer))
        self.size_column = UInt64Column(**kwargs)
        self.nested_column = nested_column
        self._write_depth_0_size = True
        super().__init__(**kwargs)
        self.null_value = []

    def size_pack(self, value):
        return self.size_struct.pack(value)

    async def size_unpack(
        self,
    ):
        return self.size_struct.unpack(await self.reader.read_bytes(self.size_struct.size))[0]

    async def write_items(
        self,
        items,
    ):
        return await self.write_data(
            items,
        )

    async def write_data(
        self,
        data,
    ):
        # Column of Array(T) is stored in "compact" format and passed to server
        # wrapped into another Array without size of wrapper array.
        self.nested_column = ArrayColumn(self.nested_column)
        self.nested_column.nullable = self.nullable
        self.nullable = False
        self._write_depth_0_size = False
        await self._write(
            data,
        )

    async def read_data(
        self,
        rows,
    ):
        self.nested_column = ArrayColumn(self.nested_column)
        self.nested_column.nullable = self.nullable
        self.nullable = False
        return await self._read(
            rows,
        )

    async def _write_sizes(
        self,
        value,
    ):
        # Level-by-level: all sizes of one nesting level are packed with a
        # single struct.pack instead of one 8-byte write per array.
        column = self
        level_values = [value]
        depth = 0
        pending_nulls = None

        while True:
            if pending_nulls is not None and column.nullable:
                await self._write_nulls_map(
                    pending_nulls,
                )

            nested_column = column.nested_column
            is_nested_array = isinstance(nested_column, ArrayColumn)
            column_nullable = column.nullable

            sizes = []
            offset = 0
            next_values = []
            nulls_map = []
            for v in level_values:
                if column_nullable:
                    v = v or []
                offset += len(v)
                if depth > 0 or self._write_depth_0_size:
                    sizes.append(offset)
                if is_nested_array:
                    for x in v:
                        next_values.append(x)
                        nulls_map.append(None if x is None else False)

            if sizes:
                await self.writer.write_bytes(Struct(f"<{len(sizes)}Q").pack(*sizes))

            if not is_nested_array:
                break
            column = nested_column
            level_values = next_values
            pending_nulls = nulls_map
            depth += 1

    async def _write_data(
        self,
        value,
    ):
        if self.nullable:
            value = value or []

        if isinstance(self.nested_column, ArrayColumn):
            value = list(chain.from_iterable(value))

        await self.nested_column._write_data(
            value,
        )

    async def _write_nulls_data(
        self,
        value,
    ):
        if self.nullable:
            value = value or []

        if isinstance(self.nested_column, ArrayColumn):
            value = list(chain.from_iterable(value))
            await self.nested_column._write_nulls_data(
                value,
            )
        else:
            if self.nested_column.nullable:
                await self.nested_column._write_nulls_map(
                    value,
                )

    async def _write(
        self,
        value,
    ):
        value = self.prepare_items(value)
        await self._write_sizes(
            value,
        )
        await self._write_nulls_data(
            value,
        )
        await self._write_data(
            value,
        )

    async def read_state_prefix(
        self,
    ):
        return await self.nested_column.read_state_prefix()

    async def write_state_prefix(
        self,
    ):
        await self.nested_column.write_state_prefix()

    async def _read(
        self,
        size,
    ):
        # Level-by-level: within one nesting level the offsets are cumulative,
        # so each level is a single struct.unpack and slice-zip instead of a
        # per-array queue walk.
        slices_series = []

        if self.nested_column.nullable:
            nulls_map = await self._read_nulls_map(
                size,
            )
        else:
            nulls_map = [0] * size

        n_items = size
        nested_column = self.nested_column

        while isinstance(nested_column, ArrayColumn):
            if n_items:
                offsets_struct = Struct(f"<{n_items}Q")
                offsets = offsets_struct.unpack(await self.reader.read_bytes_view(offsets_struct.size))
                next_n_items = offsets[-1]
            else:
                offsets = ()
                next_n_items = 0
            slices_series.append((offsets, nulls_map))

            nested_column = nested_column.nested_column
            if nested_column.nullable:
                nulls_map = await self._read_nulls_map(
                    next_n_items,
                )
            else:
                nulls_map = [0] * next_n_items
            n_items = next_n_items

        data = []
        if n_items:
            # A list, so the rebuild slices below yield lists directly.
            data = list(await nested_column._read_data(n_items, nulls_map=nulls_map))

        # Build nested structure, innermost level first. `chain((0,), offsets)`
        # zipped against `offsets` walks the boundaries pairwise without
        # materializing a slice list.
        for offsets, level_nulls_map in reversed(slices_series):
            data = [
                None if is_null else data[slice_from:slice_to]
                for slice_from, slice_to, is_null in zip(
                    chain((0,), offsets), offsets, level_nulls_map
                )
            ]

        return tuple(data)


def create_array_column(spec, column_by_spec_getter, column_options):
    inner = spec[6:-1]
    return ArrayColumn(column_by_spec_getter(inner), **column_options)

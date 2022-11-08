from asynch.proto.utils import compat

from ...errors import ColumnTypeMismatchException
from .base import FormatColumn


class IntColumn(FormatColumn):
    py_types = compat.integer_types
    int_size = None

    def __init__(self, types_check=False, **kwargs):
        super(IntColumn, self).__init__(types_check=types_check, **kwargs)

        if types_check:
            self.mask = (1 << 8 * self.int_size) - 1

            # Chop only bytes that fit current type.
            # ctypes.c_intXX is slower.
            def before_write_items(items, nulls_map=None):
                # TODO: cythonize
                null_value = self.null_value

                for i, item in enumerate(items):
                    if nulls_map and nulls_map[i]:
                        items[i] = null_value
                        continue

                    if item >= 0:
                        sign = 1
                    else:
                        sign = -1
                        item = -item

                    items[i] = sign * (item & self.mask)

            self.before_write_items = before_write_items


class UIntColumn(IntColumn):
    def __init__(self, types_check=False, **kwargs):
        super(UIntColumn, self).__init__(types_check=types_check, **kwargs)

        if types_check:

            def check_item(value):
                if value < 0:
                    raise ColumnTypeMismatchException(value)

            self.check_item = check_item


class Int8Column(IntColumn):
    ch_type = "Int8"
    format = "b"
    int_size = 1


class Int16Column(IntColumn):
    ch_type = "Int16"
    format = "h"
    int_size = 2


class Int32Column(IntColumn):
    ch_type = "Int32"
    format = "i"
    int_size = 4


class Int64Column(IntColumn):
    ch_type = "Int64"
    format = "q"
    int_size = 8


class UInt8Column(UIntColumn):
    ch_type = "UInt8"
    format = "B"
    int_size = 1


class UInt16Column(UIntColumn):
    ch_type = "UInt16"
    format = "H"
    int_size = 2


class UInt32Column(UIntColumn):
    ch_type = "UInt32"
    format = "I"
    int_size = 4


class UInt64Column(UIntColumn):
    ch_type = "UInt64"
    format = "Q"
    int_size = 8


class BigIntColumn(IntColumn):
    async def write_items(self, items):
        data = b"".join(n.to_bytes(self.int_size, "little", signed=True) for n in items)
        await self.writer.write_bytes(data)

    async def read_items(self, n_items):
        data = await self.reader.read_bytes(self.int_size * n_items)
        chunks = [
            data[i : i + self.int_size] for i in range(0, len(data), self.int_size)  # noqa:E203
        ]
        return [int.from_bytes(chunk, "little", signed=True) for chunk in chunks]


class BigUIntColumn(UIntColumn):
    async def write_items(self, items):
        data = b"".join(n.to_bytes(self.int_size, "little", signed=False) for n in items)
        await self.writer.write_bytes(data)

    async def read_items(self, n_items):
        data = await self.reader.read_bytes(self.int_size * n_items)
        chunks = [
            data[i : i + self.int_size] for i in range(0, len(data), self.int_size)  # noqa:E203
        ]
        return [int.from_bytes(chunk, "little", signed=False) for chunk in chunks]


class Int128Column(BigIntColumn):
    ch_type = "Int128"
    int_size = 16


class Int256Column(BigIntColumn):
    ch_type = "Int256"
    int_size = 32


class UInt128Column(BigUIntColumn):
    ch_type = "UInt128"
    int_size = 16


class UInt256Column(BigUIntColumn):
    ch_type = "UInt256"
    int_size = 32

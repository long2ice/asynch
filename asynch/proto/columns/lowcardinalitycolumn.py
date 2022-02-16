from math import log

from .base import Column
from .intcolumn import UInt8Column, UInt16Column, UInt32Column, UInt64Column


def create_low_cardinality_column(spec, column_by_spec_getter):
    inner = spec[15:-1]
    nested = column_by_spec_getter(inner)
    return LowCardinalityColumn(nested)


class LowCardinalityColumn(Column):
    """
    Stores column as index (unique elements) and keys.
    Good for de-duplication of large values with low cardinality.
    """

    int_types = {0: UInt8Column, 1: UInt16Column, 2: UInt32Column, 3: UInt64Column}

    # Need to read additional keys.
    # Additional keys are stored before indexes as value N and N keys
    # after them.
    has_additional_keys_bit = 1 << 9
    # Need to update dictionary.
    # It means that previous granule has different dictionary.
    need_update_dictionary = 1 << 10

    serialization_type = has_additional_keys_bit | need_update_dictionary

    def __init__(self, nested_column, **kwargs):
        kwargs.update(dict(reader=nested_column.reader, writer=nested_column.writer))
        self.nested_column = nested_column
        super(LowCardinalityColumn, self).__init__(**kwargs)

    async def read_state_prefix(
        self,
    ):
        return await self.reader.read_uint64()

    async def write_state_prefix(
        self,
    ):
        # KeysSerializationVersion. See ClickHouse docs.
        return await self.writer.write_int64(1)

    async def _write_data(
        self,
        items,
    ):
        index, keys = [], []
        key_by_index_element = {}

        if self.nested_column.nullable:
            # First element represents NULL if column is nullable.
            index.append(self.nested_column.null_value)
            # Prevent null map writing. Reset nested column nullable flag.
            self.nested_column.nullable = False

            for x in items:
                if x is None:
                    # Zero element for null.
                    keys.append(0)

                else:
                    key = key_by_index_element.get(x)
                    # Get key from index or add it to index.
                    if key is None:
                        key = len(key_by_index_element)
                        key_by_index_element[x] = key
                        index.append(x)

                    keys.append(key + 1)
        else:
            for x in items:
                key = key_by_index_element.get(x)

                # Get key from index or add it to index.
                if key is None:
                    key = len(key_by_index_element)
                    key_by_index_element[x] = len(key_by_index_element)
                    index.append(x)

                keys.append(key)

        # Do not write anything for empty column.
        # May happen while writing empty arrays.
        if not len(index):
            return

        int_type = int(log(len(index), 2) / 8)
        int_column = self.int_types[int_type](reader=self.reader, writer=self.writer)

        serialization_type = self.serialization_type | int_type
        await self.writer.write_int64(serialization_type)
        await self.writer.write_int64(len(index))

        await self.nested_column.write_data(
            index,
        )
        await self.writer.write_int64(len(items))

        await int_column.write_data(
            keys,
        )

    async def _read_data(self, n_items, nulls_map=None):
        if not n_items:
            return tuple()

        serialization_type = await self.reader.read_uint64()

        # Lowest byte contains info about key type.
        key_type = serialization_type & 0xF
        keys_column = self.int_types[key_type](reader=self.reader, writer=self.writer)

        nullable = self.nested_column.nullable
        # Prevent null map reading. Reset nested column nullable flag.
        self.nested_column.nullable = False

        index_size = await self.reader.read_uint64()
        index = await self.nested_column.read_data(
            index_size,
        )
        if nullable:
            index = (None,) + index[1:]

        await self.reader.read_uint64()  # number of keys
        keys = await keys_column.read_data(
            n_items,
        )

        return tuple(index[x] for x in keys)

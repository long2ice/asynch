# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from asynch.proto.columns.base import Column


class String(Column):
    ch_type = "String"
    py_types = (str,)
    null_value = ""
    read_as_bytes = False

    async def write_items(self, items):
        await self.writer.write_strings(items)

    async def read_items(self, n_items):
        return tuple(await self.reader.read_strs(n_items, as_bytes=self.read_as_bytes))


class ByteString(String):
    py_types = (bytes,)
    null_value = b""
    read_as_bytes = True


class FixedString(String):
    ch_type = "FixedString"
    read_as_bytes = False

    def __init__(self, reader, writer, length, **kwargs):
        self.length = length
        super().__init__(reader, writer, **kwargs)

    async def write_items(self, items):
        await self.writer.write_fixed_strings(items, self.length)

    async def read_items(self, n_items):
        return tuple(
            await self.reader.read_fixed_strs(n_items, self.length, as_bytes=self.read_as_bytes)
        )


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)
    null_value = b""
    read_as_bytes = True


def create_string_column(spec, column_options):
    client_settings = column_options["context"].client_settings
    strings_as_bytes = client_settings["strings_as_bytes"]
    if spec == "String":
        cls = ByteString if strings_as_bytes else String
        return cls(**column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length=length, **column_options)

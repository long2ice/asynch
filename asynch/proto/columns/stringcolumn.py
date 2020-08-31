from asynch.proto import constants
from asynch.proto.columns.base import Column
from asynch.proto.utils import compat


class String(Column):
    ch_type = "String"
    py_types = compat.string_types
    null_value = ""

    default_encoding = constants.STRINGS_ENCODING

    def __init__(self, encoding=default_encoding, **kwargs):
        self.encoding = encoding
        super(String, self).__init__(**kwargs)

    async def write_items(
        self, items,
    ):
        await self.writer.write_str(items,)

    async def read_items(
        self, n_items,
    ):
        return await self.reader.read_str()


class ByteString(String):
    py_types = (bytes,)
    null_value = b""

    async def write_items(
        self, items,
    ):
        await self.writer.write_str(items)

    async def read_items(
        self, n_items,
    ):
        return await self.reader.read_str()


class FixedString(String):
    ch_type = "FixedString"


class ByteFixedString(FixedString):
    py_types = (bytearray, bytes)
    null_value = b""


def create_string_column(spec, column_options):
    client_settings = column_options["context"].client_settings
    strings_as_bytes = client_settings["strings_as_bytes"]
    encoding = client_settings.get("strings_encoding", String.default_encoding)

    if spec == "String":
        cls = ByteString if strings_as_bytes else String
        return cls(encoding=encoding, **column_options)
    else:
        length = int(spec[12:-1])
        cls = ByteFixedString if strings_as_bytes else FixedString
        return cls(length, encoding=encoding, **column_options)

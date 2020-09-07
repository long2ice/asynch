import importlib
from typing import Callable

from clickhouse_cityhash.cityhash import CityHash128

from asynch.errors import ChecksumDoesntMatchError, UnknownCompressionMethod
from asynch.proto.io import BufferedReader, BufferedWriter
from asynch.proto.protocol import CompressionMethodByte


def get_compressor_cls(alg):
    try:
        module = importlib.import_module("." + alg, __name__)
        return module.Compressor

    except ImportError:
        raise UnknownCompressionMethod("Unknown compression method: '{}'".format(alg))


def get_decompressor_cls(method_type):
    if method_type == CompressionMethodByte.LZ4:
        module = importlib.import_module(".lz4", __name__)

    elif method_type == CompressionMethodByte.ZSTD:
        module = importlib.import_module(".zstd", __name__)

    else:
        raise UnknownCompressionMethod()

    return module.Decompressor


class BaseCompressor:
    """
    Partial file-like object with write method.
    """

    method = None
    method_byte = None
    compress_func: Callable

    def __init__(self, writer: BufferedWriter):
        self.writer = writer

    def get_value(self):
        value = self.writer.buffer
        return value

    def write(self, p_str):
        self.writer.write_str(p_str)

    async def get_compressed_data(self, extra_header_size):
        data = self.get_value()
        compressed = self.compress_func(data)
        header_size = extra_header_size + 4 + 4  # sizes
        await self.writer.write_uint32(header_size + len(compressed))
        await self.writer.write_uint32(len(data))
        await self.writer.write_str(compressed)
        return self.writer.buffer


class BaseDecompressor:
    method = None
    method_byte = None
    decompress_func: Callable

    def __init__(self, reader: BufferedReader):
        self.reader = reader

    def check_hash(self, compressed_data, compressed_hash):
        if CityHash128(compressed_data) != compressed_hash:
            raise ChecksumDoesntMatchError()

    async def get_decompressed_data(self, method_byte, compressed_hash, extra_header_size):
        size_with_header = await self.reader.read_uint32()
        compressed_size = size_with_header - extra_header_size - 4

        compressed = await self.reader.read_bytes(compressed_size)

        writer = BufferedWriter()
        await writer.write_uint8(method_byte)
        await writer.write_uint32(size_with_header)
        await writer.write_bytes(compressed.getvalue())
        self.check_hash(writer.buffer, compressed_hash)
        compressed = compressed.read(compressed_size - 4)
        return compressed

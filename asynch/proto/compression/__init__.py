import importlib
from typing import TYPE_CHECKING, Type

from clickhouse_cityhash.cityhash import CityHash128

from asynch.errors import ChecksumDoesntMatchError, UnknownCompressionMethod
from asynch.proto.protocol import CompressionMethodByte

if TYPE_CHECKING:
    from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


def get_compressor_cls(alg) -> Type["BaseCompressor"]:
    try:
        module = importlib.import_module("." + alg, __name__)
        return module.Compressor

    except ImportError:
        raise UnknownCompressionMethod("Unknown compression method: '{}'".format(alg))


def get_decompressor_cls(method_type) -> Type["BaseDecompressor"]:
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

    def __init__(self, writer: "BufferedWriter"):
        self.writer = writer

    def compress_data(self, data):
        raise NotImplementedError

    async def write(self, data: bytearray):
        await self.writer.write_bytes(data)

    async def get_compressed_data(self, extra_header_size: int):
        data = self.writer.buffer
        self.writer.buffer = bytearray()
        compressed = self.compress_data(data)
        header_size = extra_header_size + 4 + 4  # sizes
        await self.writer.write_uint32(header_size + len(compressed))
        await self.writer.write_uint32(len(data))
        await self.writer.write_bytes(compressed)
        return self.writer.buffer


class BaseDecompressor:
    method = None
    method_byte = None

    def __init__(self, reader: "BufferedReader", writer: "BufferedWriter"):
        self.reader = reader
        self.writer = writer

    def decompress_data(self, data, uncompressed_size):
        raise NotImplementedError

    async def get_decompressed_data(self, method_byte, compressed_hash, extra_header_size):
        size_with_header = await self.reader.read_uint32()
        compressed_size = size_with_header - extra_header_size - 4

        compressed = await self.reader.read_bytes(compressed_size)

        await self.writer.write_uint8(method_byte)
        await self.writer.write_uint32(size_with_header)
        await self.writer.write_bytes(compressed)
        if CityHash128(self.writer.buffer) != compressed_hash:
            raise ChecksumDoesntMatchError()
        reader = self.reader.__class__(reader=self.reader.reader)
        reader.buffer = compressed
        reader.current_buffer_size = len(compressed)
        uncompressed_size = await reader.read_uint32()
        compressed = compressed[4:compressed_size]
        return self.decompress_data(compressed, uncompressed_size)

import functools

from lz4 import block

from asynch.proto.compression import BaseCompressor, BaseDecompressor
from asynch.proto.protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    compress_func = functools.partial(block.compress, mode="default", store_size=False)
    mode = "default"

    def compress_data(self, data):
        return block.compress(data, store_size=False, mode=self.mode)


class Decompressor(BaseDecompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4

    def decompress_data(self, data, uncompressed_size):
        return block.decompress(data, uncompressed_size=uncompressed_size)

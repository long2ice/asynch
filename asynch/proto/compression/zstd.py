import zstd

from asynch.proto.compression import BaseCompressor, BaseDecompressor
from asynch.proto.protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD
    compress_func = zstd.compress

    def compress_data(self, data):
        return zstd.compress(bytes(data))


class Decompressor(BaseDecompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD
    decompress_func = zstd.decompress

    def decompress_data(self, data, uncompressed_size):
        return zstd.decompress(data)

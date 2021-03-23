import zstd

from asynch.proto.compression import BaseCompressor, BaseDecompressor
from asynch.proto.protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD
    compress_func = zstd.compress


class Decompressor(BaseDecompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD
    decompress_func = zstd.decompress

    async def get_decompressed_data(self, method_byte, compressed_hash, extra_header_size):
        compressed = await super(Decompressor, self).get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )

        return zstd.decompress(compressed)

import functools

from lz4 import block

from asynch.proto.compression import BaseCompressor, BaseDecompressor
from asynch.proto.protocol import CompressionMethod, CompressionMethodByte


class Compressor(BaseCompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    compress_func = functools.partial(block.compress, mode="default")


class Decompressor(BaseDecompressor):
    method = CompressionMethod.LZ4
    method_byte = CompressionMethodByte.LZ4
    decompress_func = block.decompress

    async def get_decompressed_data(self, method_byte, compressed_hash, extra_header_size):
        compressed = await super(Decompressor, self).get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )

        uncompressed_size = await self.reader.read_uint32()

        return block.decompress(compressed, uncompressed_size=uncompressed_size)

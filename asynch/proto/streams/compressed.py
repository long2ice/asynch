from clickhouse_cityhash.cityhash import CityHash128

from asynch.proto.compression import get_decompressor_cls
from asynch.proto.context import Context
from asynch.proto.io import BufferedReader, BufferedWriter
from asynch.proto.streams.native import BlockInputStream, BlockOutputStream


class CompressedBlockOutputStream(BlockOutputStream):
    def __init__(
        self,
        reader: BufferedReader,
        writer: BufferedWriter,
        context: Context,
        compressor_cls,
        compress_block_size,
    ):
        super().__init__(reader, writer, context)
        self.compressor_cls = compressor_cls
        self.compress_block_size = compress_block_size

        self.compressor = self.compressor_cls(writer)

    def get_compressed_hash(self, data):
        return CityHash128(data)

    async def finalize(self):
        await self.writer.flush()

        compressed = await self.get_compressed()
        compressed_size = len(compressed)

        compressed_hash = self.get_compressed_hash(compressed)
        await self.writer.write_uint128(compressed_hash,)

        block_size = self.compress_block_size

        i = 0
        while i < compressed_size:
            await self.writer.write_bytes(compressed[i : i + block_size])  # noqa: E203
            i += block_size

        await self.writer.flush()

    async def get_compressed(self):
        compressed = BufferedWriter()

        if self.compressor.method_byte is not None:
            await compressed.write_uint8(self.compressor.method_byte)
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        data = self.compressor.get_compressed_data(extra_header_size)
        await compressed.write_bytes(data)

        return compressed.buffer


class CompressedBlockInputStream(BlockInputStream):
    def get_compressed_hash(self, data):
        return CityHash128(data)

    async def read_block(self):
        compressed_hash = await self.reader.read_uint128()
        method_byte = await self.reader.read_uint8()

        decompressor_cls = get_decompressor_cls(method_byte)
        decompressor = decompressor_cls(self.reader)

        if decompressor.method_byte is not None:
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        return decompressor.get_decompressed_data(method_byte, compressed_hash, extra_header_size)

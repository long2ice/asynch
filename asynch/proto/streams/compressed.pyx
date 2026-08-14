# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single reader/writer safe to share between threads.
from asynch.proto import constants
from asynch.proto.compression import import_cityhash
from asynch.proto.streams.block import BlockReader, BlockWriter
from asynch.proto.streams.buffered import (
    BufferedWriter,
    CompressedBufferedReader,
    CompressedBufferedWriter,
)


class CompressedBlockWriter(BlockWriter):
    def __init__(self, reader, writer, context, compressor, compress_block_size):
        self.compressor = compressor
        self.compress_block_size = compress_block_size
        self.raw_writer = writer
        self.writer = CompressedBufferedWriter(
            compressor, writer.writer, constants.BUFFER_SIZE, timeout=writer.timeout
        )
        super().__init__(reader, self.writer, context)

    async def finalize(self):
        CityHash128 = import_cityhash()
        await self.writer.flush()

        compressed = await self.get_compressed()
        compressed_size = len(compressed)

        compressed_hash = CityHash128(compressed)
        await self.raw_writer.write_uint128(compressed_hash)

        block_size = self.compress_block_size

        i = 0
        while i < compressed_size:
            await self.raw_writer.write_bytes(compressed[i : i + block_size])
            i += block_size

        await self.raw_writer.flush()

    async def get_compressed(self):
        writer = BufferedWriter()

        if self.compressor.method_byte is not None:
            await writer.write_uint8(self.compressor.method_byte)
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        data = await self.compressor.get_compressed_data(extra_header_size)
        await writer.write_bytes(data)

        return writer.buffer


class CompressedBlockReader(BlockReader):
    def __init__(self, reader, writer, context):
        self.raw_reader = reader
        self.reader = CompressedBufferedReader(
            self.raw_reader, reader.reader, constants.BUFFER_SIZE, timeout=reader.timeout
        )
        super().__init__(self.reader, writer, context)

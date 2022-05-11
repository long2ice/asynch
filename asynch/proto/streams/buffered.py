import struct
from asyncio import StreamReader, StreamWriter

import leb128

from asynch.proto import constants
from asynch.proto.compression import BaseCompressor, get_decompressor_cls

MAX_UINT64 = (1 << 64) - 1
MAX_INT64 = (1 << 63) - 1


class BufferedWriter:
    def __init__(self, writer: StreamWriter = None, max_buffer_size: int = constants.BUFFER_SIZE):
        self.max_buffer_size = max_buffer_size
        self.writer = writer
        self.buffer = bytearray()
        self.position = 0

    async def flush(self):
        if not self.writer:
            return
        self.writer.write(self.buffer)
        self.buffer = bytearray()
        self.position = 0
        await self.writer.drain()

    async def write_bytes(self, data: bytes):
        self.buffer.extend(data)
        self.position += len(data)
        if self.position >= self.max_buffer_size:
            await self.flush()

    async def write_varint(self, data: int):
        packet = leb128.i.encode(data)
        await self.write_bytes(packet)

    async def write_str(self, data: str):
        packet = data.encode()
        await self.write_varint(len(packet))
        await self.write_bytes(packet)

    async def write_strings(self, data):
        for item in data:
            if isinstance(item, str):
                packet = item.encode()
            else:
                packet = item
            await self.write_varint(len(packet))
            await self.write_bytes(packet)

    async def write_fixed_strings(self, data, length):
        for item in data:
            if isinstance(item, str):
                packet = item.encode()
            else:
                packet = item
            await self.write_bytes(packet.ljust(length, b"\x00"))

    async def close(self):
        if not self.writer:
            return
        self.writer.close()
        await self.writer.wait_closed()

    async def write_int(self, data: int, fmt: str):
        fmt = "<" + fmt
        await self.write_bytes(struct.pack(fmt, data))

    async def write_int8(
        self,
        data: int,
    ):
        await self.write_int(data, "b")

    async def write_int16(self, data):
        await self.write_int(data, "h")

    async def write_int32(self, data):
        await self.write_int(data, "i")

    async def write_int64(self, data):
        await self.write_int(data, "q")

    async def write_uint8(self, data):
        await self.write_int(data, "B")

    async def write_uint16(self, data):
        await self.write_int(data, "H")

    async def write_uint32(
        self,
        data: int,
    ):
        await self.write_int(data, "I")

    async def write_uint64(self, data: int):
        await self.write_int(data, "Q")

    async def write_uint128(self, data: int):
        fmt = "<QQ"
        packet = struct.pack(fmt, (data >> 64) & MAX_UINT64, data & MAX_UINT64)
        await self.write_bytes(packet)


class BufferedReader:
    def __init__(self, reader: StreamReader, buffer_max_size: int = constants.BUFFER_SIZE):
        self.buffer_max_size = buffer_max_size
        self.reader = reader
        self.buffer = bytearray()
        self.current_buffer_size = 0
        self.position = 0

    async def _read_into_buffer(self):
        packet = await self.reader.read(self.buffer_max_size)
        self.buffer.extend(packet)
        self.current_buffer_size = len(self.buffer)

    def _read_one(self):
        packet = self.buffer[self.position]
        self.position += 1
        return packet

    async def read_varint(self):
        if self.position == self.current_buffer_size:
            self._reset_buffer()
            await self._read_into_buffer()
        packets = bytearray()
        while True:
            packet = self._read_one()
            packets.append(packet)
            if packet < 0x80:
                break
        return leb128.u.decode(packets)

    def _reset_buffer(self):
        self.position = 0
        self.buffer = bytearray()

    async def read_str(self, as_bytes: bool = False):
        length = await self.read_varint()
        packet = await self.read_bytes(length)
        if as_bytes:
            return packet
        return packet.decode()

    async def read_fixed_str(self, length: int, as_bytes: bool = False):
        packet = await self.read_bytes(length)
        if as_bytes:
            return packet
        return packet.decode()

    async def read_bytes(self, length: int):
        packets = bytearray()
        while length > 0:
            if self.position == self.current_buffer_size:
                self._reset_buffer()
                await self._read_into_buffer()

            read_position = self.position + length
            packet = self.buffer[self.position : read_position]  # noqa: E203
            length -= len(packet)
            self.position += len(packet)
            packets.extend(packet)

        return packets

    async def read_int(self, fmt: str):
        s = struct.Struct("<" + fmt)
        packet = await self.read_bytes(s.size)
        return s.unpack(packet)[0]

    async def read_int8(
        self,
    ):
        return await self.read_int("b")

    async def read_int16(
        self,
    ):
        return await self.read_int("h")

    async def read_int32(
        self,
    ):
        return await self.read_int("i")

    async def read_int64(
        self,
    ):
        return await self.read_int("q")

    async def read_uint8(
        self,
    ):
        return await self.read_int("B")

    async def read_uint16(
        self,
    ):
        return await self.read_int("H")

    async def read_uint32(
        self,
    ):
        return await self.read_int("I")

    async def read_uint64(
        self,
    ):
        return await self.read_int("Q")

    async def read_uint128(
        self,
    ):
        hi = await self.read_int("Q")
        lo = await self.read_int("Q")
        return (hi << 64) + lo


class CompressedBufferedWriter(BufferedWriter):
    def __init__(
        self,
        compressor: BaseCompressor,
        writer: StreamWriter = None,
        max_buffer_size: int = constants.BUFFER_SIZE,
    ):
        self.compressor = compressor
        super(CompressedBufferedWriter, self).__init__(writer, max_buffer_size)

    async def flush(self):
        await self.compressor.write(self.buffer)
        self.position = 0


class CompressedBufferedReader(BufferedReader):
    def __init__(
        self,
        raw_reader: BufferedReader,
        reader: StreamReader,
        buffer_max_size: int = constants.BUFFER_SIZE,
    ):
        self.raw_reader = raw_reader
        super(CompressedBufferedReader, self).__init__(reader, buffer_max_size)

    async def _read_compressed_data(self):
        compressed_hash = await self.raw_reader.read_uint128()
        method_byte = await self.raw_reader.read_uint8()

        decompressor_cls = get_decompressor_cls(method_byte)
        decompressor = decompressor_cls(self.raw_reader, BufferedWriter())

        if decompressor.method_byte is not None:
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        return await decompressor.get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )

    async def _read_into_buffer(self):
        self.buffer = await self._read_compressed_data()
        self.current_buffer_size = len(self.buffer)

        if self.current_buffer_size == 0:
            raise EOFError("Unexpected EOF while reading bytes")

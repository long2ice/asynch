from asyncio import StreamWriter

import leb128


class BufferedWriter:
    def __init__(self, writer: StreamWriter, buffer_max_size: int):
        self.buffer_max_size = buffer_max_size
        self.writer = writer
        self.buffer = []
        self.buffer_size = 0

    async def flush(self):
        self.writer.write(b"".join(self.buffer))
        self.buffer = []
        await self.writer.drain()

    async def _write(self, data: bytes):
        self.buffer.append(data)
        self.buffer_size += len(data)
        if self.buffer_size == self.buffer_max_size:
            await self.flush()

    async def write_varint(self, data: int):
        packet = leb128.i.encode(data)
        await self._write(packet)

    async def write_bytes(self, data: str):
        packet = data.encode()
        await self.write_varint(len(packet))
        await self._write(packet)

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()

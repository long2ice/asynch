from asyncio import StreamReader

import leb128


class BufferedReader:
    def __init__(self, reader: StreamReader, buffer_size: int):
        self.buffer_size = buffer_size
        self.reader = reader

    async def read_varint(self):
        packet = await self.reader.read(1)
        return leb128.i.decode(packet)

    async def read_str(self):
        length = await self.read_varint()
        packet = await self.read_bytes(length)
        return packet.decode()

    async def read_bytes(self, length: int):
        return await self.reader.read(length)

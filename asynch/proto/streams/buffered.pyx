# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single BufferedReader/BufferedWriter safe to share between threads.
import asyncio
import struct

from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.unicode cimport PyUnicode_DecodeUTF8

from asynch.errors import OperationalError
from asynch.proto import constants
from asynch.proto.compression import BaseCompressor, get_decompressor_cls

MAX_UINT64 = (1 << 64) - 1
MAX_INT64 = (1 << 63) - 1


def encode_varint(value):
    """Encode a non-negative integer as an unsigned LEB128 varint.

    The ClickHouse wire format uses unsigned LEB128; the previously used
    `leb128.i.encode` was the *signed* variant, which happened to be decodable
    by the server but produced non-canonical (longer) encodings for values
    whose top 7-bit group has its high bit set.
    """
    if value < 0:
        raise ValueError(f"varint must be non-negative, got {value}")
    result = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            result.append(byte | 0x80)
        else:
            result.append(byte)
            return bytes(result)


class BufferedWriter:
    def __init__(self, writer=None, max_buffer_size=constants.BUFFER_SIZE, timeout=None):
        self.max_buffer_size = max_buffer_size
        self.writer = writer
        self.buffer = bytearray()
        self.position = 0
        # `send_receive_timeout`: None disables it.
        self.timeout = timeout

    async def flush(self):
        if not self.writer:
            return
        self.writer.write(self.buffer)
        self.buffer = bytearray()
        self.position = 0
        # `drain()` is the only part of a write that waits on the peer: it
        # blocks once the transport's send buffer is full and the peer stops
        # reading.
        if self.timeout is None:
            await self.writer.drain()
        else:
            await asyncio.wait_for(self.writer.drain(), timeout=self.timeout)

    async def write_bytes(self, data):
        self.buffer.extend(data)
        self.position += len(data)
        if self.position >= self.max_buffer_size:
            await self.flush()

    async def write_varint(self, data):
        await self.write_bytes(encode_varint(data))

    async def write_str(self, data):
        packet = data.encode()
        await self.write_varint(len(packet))
        await self.write_bytes(packet)

    async def write_strings(self, data):
        # Hot path for String columns: append length-prefixed values in a sync
        # loop and only await when the buffer needs flushing. Both the buffer
        # and the position are re-read after each flush, since flushing swaps
        # in a fresh buffer.
        cdef bytearray buf = <bytearray> self.buffer
        cdef Py_ssize_t pos = self.position
        cdef Py_ssize_t max_size = self.max_buffer_size
        cdef bytes packet
        for item in data:
            if isinstance(item, str):
                packet = (<str> item).encode()
            else:
                packet = bytes(item)
            varint = encode_varint(len(packet))
            buf.extend(varint)
            buf.extend(packet)
            pos += len(varint) + len(packet)
            if pos >= max_size:
                self.position = pos
                await self.flush()
                buf = <bytearray> self.buffer
                pos = self.position
        self.position = pos

    async def write_fixed_strings(self, data, length):
        cdef bytearray buf = <bytearray> self.buffer
        cdef Py_ssize_t pos = self.position
        cdef Py_ssize_t max_size = self.max_buffer_size
        cdef bytes packet
        for item in data:
            if isinstance(item, str):
                packet = (<str> item).encode()
            else:
                packet = bytes(item)
            packet = packet.ljust(length, b"\x00")
            buf.extend(packet)
            pos += len(packet)
            if pos >= max_size:
                self.position = pos
                await self.flush()
                buf = <bytearray> self.buffer
                pos = self.position
        self.position = pos

    async def close(self):
        if not self.writer:
            return
        self.writer.close()
        await self.writer.wait_closed()

    async def write_int(self, data, fmt):
        fmt = "<" + fmt
        await self.write_bytes(struct.pack(fmt, data))

    async def write_int8(self, data):
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

    async def write_uint32(self, data):
        await self.write_int(data, "I")

    async def write_uint64(self, data):
        await self.write_int(data, "Q")

    async def write_uint128(self, data):
        fmt = "<QQ"
        packet = struct.pack(fmt, (data >> 64) & MAX_UINT64, data & MAX_UINT64)
        await self.write_bytes(packet)


class BufferedReader:
    def __init__(self, reader, buffer_max_size=constants.BUFFER_SIZE, timeout=None):
        self.buffer_max_size = buffer_max_size
        self.reader = reader
        self.buffer = bytearray()
        self.current_buffer_size = 0
        self.position = 0
        # `send_receive_timeout`: None disables it.
        self.timeout = timeout

    async def _refill_buffer(self):
        if self.position == self.current_buffer_size:
            self._reset_buffer()
            await self._read_into_buffer()

    def _is_buffer_empty(self):
        return not (self.buffer or self.position)

    async def _is_buffer_readable(self):
        await self._refill_buffer()
        if self._is_buffer_empty():
            return False
        return True

    def _reset_buffer(self):
        self.position = 0
        self.buffer = bytearray()

    async def _read_into_buffer(self):
        if self.timeout is None:
            packet = await self.reader.read(self.buffer_max_size)
        else:
            packet = await asyncio.wait_for(
                self.reader.read(self.buffer_max_size), timeout=self.timeout
            )
        self.buffer.extend(packet)
        self.current_buffer_size = len(self.buffer)

    def _read_one(self):
        packet = self.buffer[self.position]
        self.position += 1
        return packet

    async def read_varint(self):
        result = 0
        shift = 0
        while True:
            if not (await self._is_buffer_readable()):
                break
            packet = self._read_one()
            result |= (packet & 0x7F) << shift
            shift += 7
            if packet < 0x80:
                return result

        if shift:
            # The stream ended mid-varint; return what was decoded so far,
            # matching the previous lenient behavior.
            return result
        msg = "Failed to read data from socket. Likely the connection was closed by the remote."
        raise OperationalError(msg)

    async def read_bytes(self, length):
        packets = bytearray()
        while length > 0:
            if not (await self._is_buffer_readable()):
                break
            read_position = self.position + length
            packet = self.buffer[self.position : read_position]
            length -= len(packet)
            self.position += len(packet)
            packets.extend(packet)
        return packets

    async def read_fixed_str(self, length, as_bytes=False):
        packet = await self.read_bytes(length)
        if as_bytes:
            return packet
        return packet.decode()

    async def read_str(self, as_bytes=False):
        length = await self.read_varint()
        return await self.read_fixed_str(length=length, as_bytes=as_bytes)

    def _try_view(self, Py_ssize_t length):
        """Zero-copy read of `length` bytes as a memoryview, or None.

        The view aliases the receive buffer, so the caller MUST fully consume
        it (e.g. struct.unpack) before the next await on this reader - a later
        refill mutates the underlying bytearray and an exported buffer would
        raise BufferError.
        """
        cdef Py_ssize_t pos = self.position
        if self.current_buffer_size - pos >= length:
            view = memoryview(self.buffer)[pos:pos + length]
            self.position = pos + length
            return view
        return None

    async def read_bytes_view(self, length):
        """`read_bytes`, but zero-copy when the data is already buffered.

        Returns a memoryview (fast path) or a bytearray (slow path); both
        satisfy the buffer protocol for struct.unpack. See `_try_view` for the
        consume-before-next-await contract.
        """
        view = self._try_view(length)
        if view is not None:
            return view
        return await self.read_bytes(length)

    def _read_strs_sync(self, list items, Py_ssize_t n_items, bint as_bytes):
        """Append complete length-prefixed values available in the buffer.

        Stops (leaving `position` at the start of the incomplete value) as soon
        as a varint or its payload crosses the end of the buffered data; the
        caller then awaits the slow path for exactly one value and retries.
        """
        cdef const unsigned char[:] view
        cdef Py_ssize_t pos, end, p, str_len
        cdef unsigned long long length
        cdef int shift
        cdef unsigned char b

        buffer_obj = self.buffer
        if buffer_obj is None:
            return
        view = buffer_obj
        pos = self.position
        end = self.current_buffer_size
        if end > view.shape[0]:
            end = view.shape[0]

        while len(items) < n_items:
            p = pos
            length = 0
            shift = 0
            while True:
                if p >= end:
                    self.position = pos
                    return
                b = view[p]
                p += 1
                length |= (<unsigned long long> (b & 0x7F)) << shift
                shift += 7
                if b < 0x80:
                    break
            str_len = <Py_ssize_t> length
            if p + str_len > end:
                self.position = pos
                return
            if str_len == 0:
                obj = "" if not as_bytes else b""
            elif as_bytes:
                obj = PyBytes_FromStringAndSize(<const char*> &view[p], str_len)
            else:
                # Decode straight from the buffer: one object per value
                # instead of an intermediate bytes plus its decode.
                obj = PyUnicode_DecodeUTF8(<const char*> &view[p], str_len, NULL)
            items.append(obj)
            pos = p + str_len
        self.position = pos

    async def read_strs(self, n_items, as_bytes=False):
        """Bulk `read_str`: sync-parse from the buffer, await only on underflow."""
        cdef list items = []
        cdef Py_ssize_t n = n_items
        while True:
            self._read_strs_sync(items, n, as_bytes)
            if len(items) >= n:
                return items
            items.append(await self.read_str(as_bytes=as_bytes))
            if len(items) >= n:
                return items

    def _read_fixed_strs_sync(self, list items, Py_ssize_t n_items, Py_ssize_t length,
                              bint as_bytes):
        cdef const unsigned char[:] view
        cdef Py_ssize_t pos, end

        buffer_obj = self.buffer
        if buffer_obj is None:
            return
        view = buffer_obj
        pos = self.position
        end = self.current_buffer_size
        if end > view.shape[0]:
            end = view.shape[0]

        while len(items) < n_items and pos + length <= end:
            if length == 0:
                obj = "" if not as_bytes else b""
            elif as_bytes:
                obj = PyBytes_FromStringAndSize(<const char*> &view[pos], length)
            else:
                obj = PyUnicode_DecodeUTF8(<const char*> &view[pos], length, NULL)
            items.append(obj)
            pos += length
        self.position = pos

    async def read_fixed_strs(self, n_items, length, as_bytes=False):
        """Bulk `read_fixed_str`: sync-parse from the buffer, await on underflow."""
        cdef list items = []
        cdef Py_ssize_t n = n_items
        while True:
            self._read_fixed_strs_sync(items, n, length, as_bytes)
            if len(items) >= n:
                return items
            items.append(await self.read_fixed_str(length, as_bytes=as_bytes))
            if len(items) >= n:
                return items

    async def read_int(self, fmt):
        s = struct.Struct("<" + fmt)
        packet = await self.read_bytes(s.size)
        return s.unpack(packet)[0]

    async def read_int8(self):
        return await self.read_int("b")

    async def read_int16(self):
        return await self.read_int("h")

    async def read_int32(self):
        return await self.read_int("i")

    async def read_int64(self):
        return await self.read_int("q")

    async def read_uint8(self):
        return await self.read_int("B")

    async def read_uint16(self):
        return await self.read_int("H")

    async def read_uint32(self):
        return await self.read_int("I")

    async def read_uint64(self):
        return await self.read_int("Q")

    async def read_uint128(self):
        hi = await self.read_int("Q")
        lo = await self.read_int("Q")
        return (hi << 64) + lo


class CompressedBufferedWriter(BufferedWriter):
    def __init__(self, compressor, writer=None, max_buffer_size=constants.BUFFER_SIZE,
                 timeout=None):
        self.compressor = compressor
        super().__init__(writer, max_buffer_size, timeout=timeout)

    async def flush(self):
        await self.compressor.write(self.buffer)
        # Hand the compressor a fresh buffer: `compressor.write` copies what it
        # is given, so keeping the old one would re-compress and re-send every
        # byte written since the connection was opened. The block writer is a
        # connection-level singleton and flushes once per block, so a single
        # insert (external-tables block, data block, terminating empty block)
        # already corrupts the stream without this.
        self.buffer = bytearray()
        self.position = 0


class CompressedBufferedReader(BufferedReader):
    def __init__(self, raw_reader, reader, buffer_max_size=constants.BUFFER_SIZE, timeout=None):
        self.raw_reader = raw_reader
        super().__init__(reader, buffer_max_size, timeout=timeout)

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

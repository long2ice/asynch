from asyncio import StreamReader
from unittest.mock import AsyncMock

import pytest

from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


@pytest.mark.asyncio
async def test_BufferedReader_overflow():
    stream_data = b"1234"

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader, 1)

    result = await reader.read_bytes(4)

    assert result == stream_data


@pytest.mark.asyncio
async def test_BufferedWriter_overflow(mocker):
    writer = mocker.Mock()
    writer.drain = AsyncMock()
    b_writer = BufferedWriter(writer, 1)

    await b_writer.write_bytes(b"1234")

    writer.write.assert_called_with(b"1234")
    writer.drain.assert_awaited_once()
    assert b_writer.position == 0
    assert len(b_writer.buffer) == 0


@pytest.mark.asyncio
async def test_ByfferedWriter_write_fixed_strings(mocker):
    writer = mocker.Mock()
    writer.drain = AsyncMock()

    b_writer = BufferedWriter(writer, 1024)

    await b_writer.write_fixed_strings(["", "12", b"12"], 2)

    assert b_writer.buffer == b"\x00\x001212"

from asyncio import StreamReader

import pytest

from asynch.proto.io import BufferedReader, BufferedWriter


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
    writer.drain = mocker.AsyncMock()
    reader = BufferedWriter(writer, 1)

    await reader.write_bytes(b"1234")

    writer.write.assert_called_with(b"1234")
    writer.drain.assert_awaited_once()
    assert reader.position == 0
    assert len(reader.buffer) == 0

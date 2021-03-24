import pytest
from asyncio import StreamReader

from asynch.proto.io import BufferedReader


@pytest.mark.asyncio
async def test_BufferedReader_overflow():
    stream_data = b'1234'

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader, 1)

    result = await reader.read_bytes(4)

    assert result == stream_data

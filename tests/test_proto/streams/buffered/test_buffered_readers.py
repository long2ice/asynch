from asyncio import StreamReader

import pytest

from asynch.proto.streams.buffered import BufferedReader


@pytest.mark.parametrize(
    ("stream_data", "answer"),
    [
        (b"9", 57),
        (b"32", 51),
    ],
)
async def test_read_varint(stream_data: bytes, answer: bytes):
    """When `(b"", 0)`, the reading gets stuck."""

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader)

    result = await reader.read_varint()

    assert answer == result


@pytest.mark.parametrize(
    ("stream_data", "bytes_to_read", "answer"),
    [
        (b"", 0, b""),
        (b"02", 1, b"0"),
        (b"3456", 4, b"3456"),
    ],
)
async def test_read_bytes(stream_data: bytes, bytes_to_read: int, answer: bytes):
    """If `bytes_to_read > len(stream_data)`, the reading gets stuck."""

    stream_reader = StreamReader()
    stream_reader.feed_data(stream_data)
    reader = BufferedReader(stream_reader, 1)

    result = await reader.read_bytes(bytes_to_read)

    assert answer == result

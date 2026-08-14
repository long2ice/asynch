from asyncio import StreamReader

import pytest

from asynch.proto.streams.buffered import BufferedReader, encode_varint


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        (0, b"\x00"),
        (1, b"\x01"),
        (100, b"\x64"),  # the old signed encoder emitted the non-canonical b"\xe4\x00"
        (127, b"\x7f"),
        (128, b"\x80\x01"),
        (300, b"\xac\x02"),
        (2**63 - 1, b"\xff\xff\xff\xff\xff\xff\xff\xff\x7f"),
        (2**64 - 1, b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01"),
    ],
)
async def test_encode_canonical_and_roundtrip(value: int, encoded: bytes):
    assert encode_varint(value) == encoded

    stream_reader = StreamReader()
    stream_reader.feed_data(encoded)
    reader = BufferedReader(stream_reader)
    assert await reader.read_varint() == value


def test_encode_negative_rejected():
    with pytest.raises(ValueError):
        encode_varint(-1)


async def test_decode_accepts_non_canonical():
    # Non-canonical encoding of 100 as produced by the old signed encoder.
    stream_reader = StreamReader()
    stream_reader.feed_data(b"\xe4\x00")
    reader = BufferedReader(stream_reader)
    assert await reader.read_varint() == 100

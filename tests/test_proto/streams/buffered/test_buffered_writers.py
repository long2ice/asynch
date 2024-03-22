from unittest.mock import AsyncMock

import pytest

from asynch.proto.streams.buffered import BufferedWriter


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

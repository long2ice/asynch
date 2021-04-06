from asyncio.streams import StreamReader

import pytest

from asynch.proto import constants
from asynch.proto.context import Context
from asynch.proto.io import BufferedReader, BufferedWriter


@pytest.fixture
def column_options():
    reader = BufferedReader(StreamReader(), constants.BUFFER_SIZE)
    writer = BufferedWriter()
    context = Context()
    context.client_settings = {
        "strings_as_bytes": False,
        "strings_encoding": constants.STRINGS_ENCODING,
    }
    column_options = {"reader": reader, "writer": writer, "context": context}
    return column_options

import asyncio
import functools
import logging
import socket
import struct
from asyncio import StreamReader, StreamWriter

import pytest

from asynch import Pool
from tests.conftest import CONNECTION_DSN

HOST = "localhost"
PORT = 9001
TIMEOUT = 1  # in seconds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@pytest.fixture(params=["graceful", "ungraceful"])
async def proxy(request):
    """Start a self-killing proxy-server.

    The connection gets killed TIMEOUT seconds.
    It can kill the connection either gracefully (TCP FIN packet), or ungracefully (TCP RST packet).
    """

    handler = functools.partial(handle_proxy, 9000, request.param == "graceful")
    server = await asyncio.start_server(handler, host=HOST, port=PORT)
    async with server:
        logger.info(f"Proxy {server} started")
        try:
            server = asyncio.create_task(server.serve_forever())
            yield
        finally:
            server.cancel()
    await asyncio.sleep(0.1)  # Avoids error "Task was destroyed but it is pending!"


@pytest.fixture()
async def proxy_pool(proxy):
    async with Pool(minsize=1, maxsize=1, dsn=CONNECTION_DSN.replace("9000", "9001")) as pool:
        yield pool


@pytest.mark.asyncio
async def test_reconnection(proxy_pool):
    async with proxy_pool.connection() as c:
        async with c.cursor() as cursor:
            await cursor.execute("SELECT 1")

    await asyncio.sleep(TIMEOUT * 2)

    async with proxy_pool.connection() as c:
        async with c.cursor() as cursor:
            await cursor.execute("SELECT 1")


@pytest.mark.asyncio
async def test_close_disconnected_connection(proxy_pool):
    async with proxy_pool.connection() as c:
        async with c.cursor() as cursor:
            await cursor.execute("SELECT 1")

    await asyncio.sleep(TIMEOUT * 2)


async def reader_to_writer(name: str, graceful: bool, reader: StreamReader, writer: StreamWriter):
    while True:
        try:
            data = await asyncio.wait_for(reader.read(2**12), timeout=TIMEOUT)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        except asyncio.TimeoutError:
            logger.info("Timeout")
            break

    if not graceful:
        sock = writer.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))

    writer.close()
    await writer.wait_closed()
    logger.info(f"{name} closed the writer.")


async def handle_proxy(
    dst_port: int, graceful: bool, src_reader: StreamReader, src_writer: StreamWriter
):
    dst_reader, dst_writer = await asyncio.open_connection(HOST, dst_port)

    connstr = f"{HOST}:{dst_port}"
    logger.info(f"Opened connection to {connstr}")

    src_dst = reader_to_writer("SRC->DST", graceful, src_reader, dst_writer)
    dst_src = reader_to_writer("DST->SRC", graceful, dst_reader, src_writer)
    await asyncio.gather(src_dst, dst_src)

    logger.info(f"{connstr} is closed.")

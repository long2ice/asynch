import asyncio
import functools
import logging
import socket
import struct
from asyncio import StreamReader, StreamWriter

import pytest

from asynch import Pool
from tests.conftest import CONNECTION_DSN

logger = logging.getLogger(__name__)
TIMEOUT_SECONDS = 1


@pytest.fixture(params=["graceful", "ungraceful"])
async def proxy(request):
    """Start a proxy server from port 9001 to 9000 that kills the connection after it idled for TIMEOUT_SECONDS seconds.
    It can kill the connection either gracefully (TCP FIN packet), or ungracefully (TCP RST packet)."""

    handler = functools.partial(handle_proxy, 9000, request.param == "graceful")
    server = await asyncio.start_server(handler, host="localhost", port=9001)
    logger.info('Started proxy server')
    async with server:
        try:
            server = asyncio.create_task(server.serve_forever())
            yield
        finally:
            server.cancel()
    await asyncio.sleep(0.1)  # Avoids error "Task was destroyed but it is pending!"


@pytest.mark.asyncio
async def test_reconnection(proxy):
    async with Pool(minsize=1, maxsize=1, dsn=CONNECTION_DSN.replace("9000", "9001")) as pool:
        async with pool.connection() as c:
            async with c.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                assert ret == (1,)

        await asyncio.sleep(TIMEOUT_SECONDS * 2)

        async with pool.connection() as c:
            async with c.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                assert ret == (1,)


@pytest.mark.asyncio
async def test_close_disconnected_connection(proxy):
    async with Pool(minsize=1, maxsize=1, dsn=CONNECTION_DSN.replace("9000", "9001")) as pool:
        async with pool.connection() as c:
            async with c.cursor() as cursor:
                await cursor.execute("SELECT 1")
                ret = await cursor.fetchone()
                assert ret == (1,)

        await asyncio.sleep(TIMEOUT_SECONDS * 2)


async def reader_to_writer(name: str, graceful: bool, reader: StreamReader, writer: StreamWriter):
    while True:
        try:
            if not name == "DST->SRC":
                data = await asyncio.wait_for(reader.read(2 ** 12), timeout=TIMEOUT_SECONDS)
            else:
                data = await reader.read(2 ** 12)

            if not data:
                break
            writer.write(data)
            await writer.drain()

        except asyncio.TimeoutError:
            logger.info("Timeout")
            break

    if not graceful:
        sock = writer.get_extra_info("socket")
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))

    writer.close()
    await writer.wait_closed()
    logger.info(f'{name} Closed connection to writer.')


async def handle_proxy(dst_port: int, graceful: bool, src_reader: StreamReader, src_writer: StreamWriter):
    dst_reader, dst_writer = await asyncio.open_connection('localhost', dst_port)
    logger.info(f'Opened connection to {dst_port}')
    src_dst = reader_to_writer('SRC->DST', graceful, src_reader, dst_writer)
    dst_src = reader_to_writer('DST->SRC', graceful, dst_reader, src_writer)
    await asyncio.gather(src_dst, dst_src)


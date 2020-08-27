import asyncio
import logging
from asyncio import StreamWriter, StreamReader
from typing import Optional
from urllib.parse import urlparse

import leb128

from asynch import constants
from asynch.protocol import Compression, ClientPacket, ServerPacket

logger = logging.getLogger(__name__)


class Connection:

    def __init__(self, host: str = '127.0.0.1', port: int = 9000, database: str = 'default', user: str = 'default',
                 password: str = '',
                 client_name: str = constants.CLIENT_NAME,
                 connect_timeout: int = constants.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
                 send_receive_timeout: int = constants.DBMS_DEFAULT_TIMEOUT_SEC,
                 sync_request_timeout: int = constants.DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
                 compress_block_size=constants.DEFAULT_COMPRESS_BLOCK_SIZE,
                 compression: bool = False,
                 secure: bool = False,
                 # Secure socket parameters.
                 verify: bool = True, ssl_version=None, ca_certs=None, ciphers=None,
                 alt_hosts: str = None):
        if secure:
            default_port = constants.DEFAULT_SECURE_PORT
        else:
            default_port = constants.DEFAULT_PORT
        self.hosts = [(host, port or default_port)]
        if alt_hosts:
            for host in alt_hosts.split(','):
                url = urlparse('clickhouse://' + host)
                self.hosts.append((url.hostname, url.port or default_port))
        self.database = database
        self.user = user
        self.password = password
        self.client_name = constants.DBMS_NAME + ' ' + client_name
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.sync_request_timeout = sync_request_timeout
        self.secure_socket = secure
        self.verify_cert = verify

        ssl_options = {}
        if ssl_version is not None:
            ssl_options['ssl_version'] = ssl_version
        if ca_certs is not None:
            ssl_options['ca_certs'] = ca_certs
        if ciphers is not None:
            ssl_options['ciphers'] = ciphers
        self.ssl_options = ssl_options
        # Use LZ4 compression by default.
        if compression is True:
            compression = 'lz4'

        if compression is False:
            self.compression = Compression.DISABLED
            self.compressor_cls = None
            self.compress_block_size = None
        else:
            self.compression = Compression.ENABLED
            # self.compressor_cls = get_compressor_cls(compression)
            self.compress_block_size = compress_block_size
        self.connected = False
        self.reader: Optional[StreamReader] = None
        self.writer: Optional[StreamWriter] = None

    def _write_varint(self, number: int):
        packet = leb128.i.encode(number)
        self.writer.write(packet)

    def _write_bytes(self, data: str):
        self.writer.write(data.encode())

    async def _read_varint(self):
        packet = await self.reader.read()
        return packet

    async def send_hello(self):
        self._write_varint(ClientPacket.HELLO)
        self._write_bytes(self.client_name)
        self._write_varint(constants.CLIENT_VERSION_MAJOR)
        self._write_varint(constants.CLIENT_VERSION_MINOR)
        self._write_varint(constants.CLIENT_REVISION)
        self._write_bytes(self.database)
        self._write_bytes(self.user)
        self._write_bytes(self.password)
        await self.writer.drain()

    async def receive_hello(self):
        data = await self._read_varint()
        print(data)

    async def ping(self):
        self._write_varint(ClientPacket.PING)
        await self.writer.drain()
        packet_type = await self._read_varint()
        print(packet_type)

    async def receive_progress(self):
        pass

    async def send_cancel(self):
        pass

    async def send_query(self):
        pass

    async def _init_connection(self, host: str, port: int):
        self.reader, self.writer = await asyncio.open_connection(host, port)
        self.connected = True
        await self.send_hello()
        await self.receive_hello()

    async def disconnect(self):
        if self.connected:
            self.connected = False
            self.writer.close()
            await self.writer.wait_closed()

    async def connect(self):
        if self.connected:
            await self.disconnect()
        logger.debug(
            'Connecting. Database: %s. User: %s', self.database, self.user
        )
        for host, port in self.hosts:
            logger.debug('Connecting to %s:%s', host, port)
            return await self._init_connection(host, port)

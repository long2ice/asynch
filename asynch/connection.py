import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

from asynch import constants
from asynch.exceptions import UnexpectedPacketFromServerError
from asynch.protocol import ClientPacket, Compression, ServerPacket
from asynch.reader import BufferedReader
from asynch.writer import BufferedWriter

logger = logging.getLogger(__name__)


class ServerInfo:
    def __init__(
        self,
        name: str,
        version_major: int,
        version_minor: int,
        version_patch: int,
        revision: int,
        timezone: str,
        display_name: str,
    ):
        self.name = name
        self.version_major = version_major
        self.version_minor = version_minor
        self.version_patch = version_patch
        self.revision = revision
        self.timezone = timezone
        self.display_name = display_name

    def version_tuple(self):
        return self.version_major, self.version_minor, self.version_patch


class Connection:
    def __init__(  # nosec:B107
        self,
        host: str = "127.0.0.1",
        port: int = 9000,
        database: str = "default",
        user: str = "default",
        password: str = "",
        client_name: str = constants.CLIENT_NAME,
        connect_timeout: int = constants.DBMS_DEFAULT_CONNECT_TIMEOUT_SEC,
        send_receive_timeout: int = constants.DBMS_DEFAULT_TIMEOUT_SEC,
        sync_request_timeout: int = constants.DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
        compress_block_size: int = constants.DEFAULT_COMPRESS_BLOCK_SIZE,
        compression: bool = False,
        secure: bool = False,
        # Secure socket parameters.
        verify: bool = True,
        ssl_version=None,
        ca_certs=None,
        ciphers=None,
        alt_hosts: str = None,
    ):
        if secure:
            default_port = constants.DEFAULT_SECURE_PORT
        else:
            default_port = constants.DEFAULT_PORT
        self.hosts = [(host, port or default_port)]
        if alt_hosts:
            for host in alt_hosts.split(","):
                url = urlparse("clickhouse://" + host)
                self.hosts.append((url.hostname, url.port or default_port))
        self.database = database
        self.user = user
        self.password = password
        self.client_name = constants.DBMS_NAME + " " + client_name
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.sync_request_timeout = sync_request_timeout
        self.secure_socket = secure
        self.verify_cert = verify

        ssl_options = {}
        if ssl_version is not None:
            ssl_options["ssl_version"] = ssl_version
        if ca_certs is not None:
            ssl_options["ca_certs"] = ca_certs
        if ciphers is not None:
            ssl_options["ciphers"] = ciphers
        self.ssl_options = ssl_options
        # Use LZ4 compression by default.
        if compression is True:
            compression = "lz4"

        if compression is False:
            self.compression = Compression.DISABLED
            self.compressor_cls = None
            self.compress_block_size = None
        else:
            self.compression = Compression.ENABLED
            # self.compressor_cls = get_compressor_cls(compression)
            self.compress_block_size = compress_block_size
        self.connected = False
        self.reader: Optional[BufferedReader] = None
        self.writer: Optional[BufferedWriter] = None
        self.server_info: Optional[ServerInfo] = None

    async def send_hello(self):
        await self.writer.write_varint(ClientPacket.HELLO)
        await self.writer.write_bytes(self.client_name)
        await self.writer.write_varint(constants.CLIENT_VERSION_MAJOR)
        await self.writer.write_varint(constants.CLIENT_VERSION_MINOR)
        await self.writer.write_varint(constants.CLIENT_REVISION)
        await self.writer.write_bytes(self.database)
        await self.writer.write_bytes(self.user)
        await self.writer.write_bytes(self.password)
        await self.writer.flush()

    def get_server(self):
        return "{}:{}".format(self.host, self.port)

    async def parse_read_exception(self):
        pass

    def unexpected_packet_message(self, expected, packet_type):
        packet_type = ServerPacket.to_str(packet_type)

        return "Unexpected packet from server {} (expected {}, got {})".format(
            self.get_server(), expected, packet_type
        )

    async def receive_hello(self):
        packet_type = await self.reader.read_varint()
        if packet_type == ServerPacket.HELLO:
            server_name = await self.reader.read_str()
            server_version_major = await self.reader.read_varint()
            server_version_minor = await self.reader.read_varint()
            server_revision = await self.reader.read_varint()
            server_timezone = None
            if server_revision >= constants.DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                server_timezone = await self.reader.read_str()

            server_display_name = ""
            if server_revision >= constants.DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME:
                server_display_name = await self.reader.read_str()

            server_version_patch = server_revision
            if server_revision >= constants.DBMS_MIN_REVISION_WITH_VERSION_PATCH:
                server_version_patch = await self.reader.read_varint()

            self.server_info = ServerInfo(
                server_name,
                server_version_major,
                server_version_minor,
                server_version_patch,
                server_revision,
                server_timezone,
                server_display_name,
            )
            self.server_info = self.server_info

            logger.debug(
                "Connected to %s server version %s.%s.%s, revision: %s",
                server_name,
                server_version_major,
                server_version_minor,
                server_version_patch,
                server_revision,
            )
        elif packet_type == ServerPacket.EXCEPTION:
            raise self.parse_read_exception()
        else:
            await self.disconnect()
            message = self.unexpected_packet_message("Hello or Exception", packet_type)
            raise UnexpectedPacketFromServerError(message)

    async def ping(self):
        await self.writer.write_varint(ClientPacket.PING)
        packet_type = await self.reader.read_varint()
        while packet_type == ServerPacket.PROGRESS:
            await self.receive_progress()
            packet_type = await self.reader.read_varint()
        if packet_type != ServerPacket.PONG:
            msg = self.unexpected_packet_message("Pong", packet_type)
            raise UnexpectedPacketFromServerError(msg)
        return True

    async def receive_progress(self):
        pass

    async def send_cancel(self):
        pass

    async def send_query(self):
        pass

    async def _init_connection(self, host: str, port: int):
        self.host, self.port = host, port
        reader, writer = await asyncio.open_connection(host, port)
        self.writer = BufferedWriter(writer, constants.BUFFER_SIZE)
        self.reader = BufferedReader(reader, constants.BUFFER_SIZE)
        self.connected = True
        await self.send_hello()
        await self.receive_hello()

    async def disconnect(self):
        if self.connected:
            self.connected = False
            await self.writer.close()

    async def connect(self):
        if self.connected:
            await self.disconnect()
        logger.debug("Connecting. Database: %s. User: %s", self.database, self.user)
        for host, port in self.hosts:
            logger.debug("Connecting to %s:%s", host, port)
            return await self._init_connection(host, port)

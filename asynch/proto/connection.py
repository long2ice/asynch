import asyncio
import logging
from typing import Optional
from urllib.parse import urlparse

from asynch.proto import constants
from asynch.proto.context import Context
from asynch.proto.cs import ClientInfo, QueryKind, ServerInfo
from asynch.proto.exceptions import UnexpectedPacketFromServerError, UnknownPacketFromServerError
from asynch.proto.io import BufferedReader, BufferedWriter
from asynch.proto.protocol import ClientPacket, Compression, ServerPacket
from asynch.proto.settings import write_settings

logger = logging.getLogger(__name__)


class QueryProcessingStage:
    """
    Determines till which state SELECT query should be executed.
    """

    FETCH_COLUMNS = 0
    WITH_MERGEABLE_STATE = 1
    COMPLETE = 2


class Packet:
    def __init__(self):
        self.type = None
        self.block = None
        self.exception = None
        self.progress = None
        self.profile_info = None
        self.multistring_message = None


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
            self.compressor_cls = get_compressor_cls(compression)
            self.compress_block_size = compress_block_size
        self.connected = False
        self.reader: Optional[BufferedReader] = None
        self.writer: Optional[BufferedWriter] = None
        self.server_info: Optional[ServerInfo] = None
        self.context = Context()
        # Block writer/reader
        self.block_in: Optional[BufferedReader] = None
        self.block_out: Optional[BufferedReader] = None

    def get_block_in_stream(self):
        if self.compression:
            from asynch.proto.streams.compressed import CompressedBlockInputStream

            return CompressedBlockInputStream(self.fin, self.context)
        else:
            from asynch.proto.streams.native import BlockInputStream

            return BlockInputStream(self.fin, self.context)

    def get_block_out_stream(self):
        if self.compression:
            from .streams.compressed import CompressedBlockOutputStream

            return CompressedBlockOutputStream(
                self.compressor_cls, self.compress_block_size, self.fout, self.context
            )
        else:
            from .streams.native import BlockOutputStream

            return BlockOutputStream(self.fout, self.context)

    async def send_hello(self):
        await self.writer.write_varint(ClientPacket.HELLO)
        await self.writer.write_str(self.client_name)
        await self.writer.write_varint(constants.CLIENT_VERSION_MAJOR)
        await self.writer.write_varint(constants.CLIENT_VERSION_MINOR)
        await self.writer.write_varint(constants.CLIENT_REVISION)
        await self.writer.write_str(self.database)
        await self.writer.write_str(self.user)
        await self.writer.write_str(self.password)
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
            self.context.server_info = self.server_info

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
        await self.writer.flush()
        packet_type = await self.reader.read_varint()
        while packet_type == ServerPacket.PROGRESS:
            await self.receive_progress()
            packet_type = await self.reader.read_varint()
        if packet_type != ServerPacket.PONG:
            msg = self.unexpected_packet_message("Pong", packet_type)
            raise UnexpectedPacketFromServerError(msg)
        return True

    async def receive_data(self):
        revision = self.server_info.revision

        if revision >= constants.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            await self.reader.read_str()

        return self.block_in.read()

    async def receive_exception(self):
        pass

    async def receive_profile_info(self):
        pass

    async def receive_multistring_message(self, packet_type: int):
        pass

    async def receive_packet(self):
        packet = Packet()

        packet.type = packet_type = await self.reader.read_varint()

        if packet_type == ServerPacket.DATA:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacket.EXCEPTION:
            packet.exception = await self.receive_exception()

        elif packet.type == ServerPacket.PROGRESS:
            packet.progress = await self.receive_progress()

        elif packet.type == ServerPacket.PROFILE_INFO:
            packet.profile_info = await self.receive_profile_info()

        elif packet_type == ServerPacket.TOTALS:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacket.EXTREMES:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacket.LOG:
            block = await self.receive_data()

        elif packet_type == ServerPacket.END_OF_STREAM:
            pass

        elif packet_type == ServerPacket.TABLE_COLUMNS:
            packet.multistring_message = await self.receive_multistring_message(packet_type)

        else:
            await self.disconnect()
            raise UnknownPacketFromServerError(
                "Unknown packet {} from server {}".format(packet_type, self.get_server())
            )

        return packet

    async def receive_progress(self):
        pass

    async def send_cancel(self):
        await self.writer.write_varint(ClientPacket.CANCEL)
        await self.writer.flush()

    async def send_query(self, query: str, query_id: str = ""):
        await self.writer.write_varint(ClientPacket.QUERY)
        await self.writer.write_str(query_id)
        revision = self.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name)
            client_info.query_kind = QueryKind.INITIAL_QUERY
            await client_info.write(revision, self.writer)

        settings_as_strings = (
            revision >= constants.DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        )
        await write_settings(self.writer, self.context.settings, settings_as_strings)

        await self.writer.write_varint(QueryProcessingStage.COMPLETE)
        await self.writer.write_varint(self.compression)

        await self.writer.write_str(query,)

        logger.debug("Query: %s", query)

        await self.writer.flush()

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

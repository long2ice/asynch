import asyncio
import logging
import ssl
from time import time
from types import GeneratorType
from typing import AsyncGenerator, Optional, Union
from urllib.parse import urlparse

from asynch.errors import (
    PartiallyConsumedQueryError,
    ServerException,
    UnexpectedPacketFromServerError,
    UnknownPacketFromServerError,
)
from asynch.proto import constants
from asynch.proto.block import BlockStreamProfileInfo, ColumnOrientedBlock, RowOrientedBlock
from asynch.proto.compression import get_compressor_cls
from asynch.proto.context import Context, ExecuteContext
from asynch.proto.cs import ClientInfo, QueryKind, ServerInfo
from asynch.proto.progress import Progress
from asynch.proto.protocol import ClientPacket, Compression, ServerPacket
from asynch.proto.result import IterQueryResult, ProgressQueryResult, QueryInfo, QueryResult
from asynch.proto.settings import write_settings
from asynch.proto.streams.block import BlockReader, BlockWriter
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter
from asynch.proto.utils.escape import escape_params
from asynch.proto.utils.helpers import chunks, column_chunks

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
    log_priorities = (
        "Unknown",
        "Fatal",
        "Critical",
        "Error",
        "Warning",
        "Notice",
        "Information",
        "Debug",
        "Trace",
    )

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
        compression: Union[bool, str] = False,
        secure: bool = False,
        # Secure socket parameters.
        verify: bool = True,
        ssl_version=None,
        ca_certs=None,
        ciphers=None,
        alt_hosts: str = None,
        stack_track=False,
        settings_is_important=False,
        **kwargs,
    ):
        self.stack_track = stack_track
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
        self.host = None
        self.port = None
        self.user = user
        self.password = password
        self.client_name = constants.DBMS_NAME + " " + client_name
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self.sync_request_timeout = sync_request_timeout
        self.settings_is_important = settings_is_important
        self._lock = asyncio.Lock()
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
        self.block_reader: Optional[BlockReader] = None
        self.block_writer: Optional[BlockWriter] = None
        self.block_reader_raw: Optional[BlockReader] = None
        self.is_query_executing = False
        self.client_trace_context = None

        self.settings = kwargs.pop("settings", {}).copy()
        self.client_settings = {
            "insert_block_size": int(
                self.settings.pop(
                    "insert_block_size",
                    constants.DEFAULT_INSERT_BLOCK_SIZE,
                )
            ),
            "strings_as_bytes": self.settings.pop("strings_as_bytes", False),
            "strings_encoding": self.settings.pop("strings_encoding", constants.STRINGS_ENCODING),
            "use_numpy": self.settings.pop("use_numpy", False),
            "opentelemetry_traceparent": self.settings.pop("opentelemetry_traceparent", None),
            "opentelemetry_tracestate": self.settings.pop("opentelemetry_tracestate", ""),
            "quota_key": self.settings.pop("quota_key", ""),
        }
        self.last_query: Optional[QueryInfo] = None
        self.available_client_settings = (
            "insert_block_size",  # TODO: rename to max_insert_block_size
            "strings_as_bytes",
            "strings_encoding",
            "use_numpy",
            "opentelemetry_traceparent",
            "opentelemetry_tracestate",
            "quota_key",
        )
        self.context.settings = self.settings
        self.context.client_settings = self.client_settings

    def get_block_reader(self):
        if self.compression:
            from asynch.proto.streams.compressed import CompressedBlockReader

            return CompressedBlockReader(
                self.reader,
                self.writer,
                self.context,
            )
        else:
            from asynch.proto.streams.block import BlockReader

            return BlockReader(self.reader, self.writer, self.context)

    def get_block_writer(self):
        if self.compression:
            compressor = self.compressor_cls(BufferedWriter())
            from .streams.compressed import CompressedBlockWriter

            return CompressedBlockWriter(
                self.reader,
                self.writer,
                self.context,
                compressor,
                self.compress_block_size,
            )
        else:
            from .streams.block import BlockWriter

            return BlockWriter(self.reader, self.writer, self.context)

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
            raise await self.read_exception()
        else:
            await self.disconnect()
            message = self.unexpected_packet_message("Hello or Exception", packet_type)
            raise UnexpectedPacketFromServerError(message)

    def _get_ssl_context(self):
        if not self.secure_socket:
            return None
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_version = self.ssl_options.get("ssl_version")
        if ssl_version:
            ssl_ctx.options |= ssl_version
        ca_certs = self.ssl_options.get("ca_certs")
        if ca_certs:
            ssl_ctx.load_verify_locations(ca_certs)
        else:
            ssl_ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
        ciphers = self.ssl_options.get("ciphers")
        if ciphers:
            ssl_ctx.set_ciphers(ciphers)
        if self.verify_cert:
            ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        return ssl_ctx

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

    async def receive_data(self, raw=False):
        revision = self.server_info.revision

        if revision >= constants.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            await self.reader.read_str()

        return await (self.block_reader_raw if raw else self.block_reader).read()

    async def receive_exception(self):
        return await self.read_exception()

    async def read_exception(self, additional_message=None):
        code = await self.reader.read_int32()
        name = await self.reader.read_str()
        message = await self.reader.read_str()
        stack_trace = await self.reader.read_str()
        has_nested = bool(await self.reader.read_uint8())

        new_message = ""

        if additional_message:
            new_message += additional_message + ". "

        if name != "DB::Exception":
            new_message += name + ". "

        new_message += message
        if self.stack_track:
            new_message += ". Stack trace:\n\n" + stack_trace
        nested = None
        if has_nested:
            nested = await self.read_exception()

        return ServerException(new_message, code, nested=nested)

    async def receive_profile_info(self):
        profile_info = BlockStreamProfileInfo(self.reader)
        await profile_info.read()
        return profile_info

    async def receive_multistring_message(self, packet_type: int):
        num = ServerPacket.strings_in_message(packet_type)
        return [await self.reader.read_str() for _i in range(num)]

    def log_block(self, block):
        column_names = [x[0] for x in block.columns_with_types]

        for row in block.get_rows():
            row = dict(zip(column_names, row))

            if 1 <= row["priority"] <= 8:
                priority = self.log_priorities[row["priority"]]
            else:
                priority = row[0]

            # thread_number in servers prior 20.x
            thread_id = row.get("thread_id") or row["thread_number"]

            logger.info(
                "[ %s ] [ %s ] {%s} <%s> %s: %s",
                row["host_name"],
                thread_id,
                row["query_id"],
                priority,
                row["source"],
                row["text"],
            )

    async def receive_packet(self):
        packet = await self._receive_packet()

        if packet.type == ServerPacket.EXCEPTION:
            raise packet.exception

        elif packet.type == ServerPacket.PROGRESS:
            self.last_query.store_progress(packet.progress)
            return packet

        elif packet.type == ServerPacket.END_OF_STREAM:
            return False

        elif packet.type == ServerPacket.DATA:
            return packet

        elif packet.type == ServerPacket.TOTALS:
            return packet

        elif packet.type == ServerPacket.EXTREMES:
            return packet

        elif packet.type == ServerPacket.PROFILE_INFO:
            self.last_query.store_profile(packet.profile_info)
            return True

        else:
            return True

    async def _receive_packet(self):
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
            block = await self.receive_data(raw=True)
            self.log_block(block)
        elif packet_type == ServerPacket.END_OF_STREAM:
            self.is_query_executing = False
        elif packet_type == ServerPacket.TABLE_COLUMNS:
            packet.multistring_message = await self.receive_multistring_message(packet_type)
        elif packet_type == ServerPacket.PART_UUIDS:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacket.READ_TASK_REQUEST:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacket.PROFILE_EVENTS:
            packet.block = await self.receive_data(raw=True)
        else:
            await self.disconnect()
            raise UnknownPacketFromServerError(
                "Unknown packet {} from server {}".format(packet_type, self.get_server())
            )

        return packet

    async def packet_generator(self) -> AsyncGenerator:
        while True:
            packet = await self.receive_packet()
            if not packet:
                break

            if packet is True:
                continue

            yield packet

    async def receive_result(self, with_column_types=False, progress=False, columnar=False):

        generator = self.packet_generator()

        if progress:
            return ProgressQueryResult(
                self.reader, generator, with_column_types=with_column_types, columnar=columnar
            )

        else:
            result = QueryResult(
                self.reader, generator, with_column_types=with_column_types, columnar=columnar
            )
            return await result.get_result()

    async def receive_progress(self):
        progress = Progress(self.reader)
        await progress.read(
            self.server_info.revision,
        )
        return progress

    async def send_cancel(self):
        await self.writer.write_varint(ClientPacket.CANCEL)
        await self.writer.flush()

    async def send_query(self, query: str, query_id: str = ""):
        await self.writer.write_varint(ClientPacket.QUERY)
        await self.writer.write_str(query_id)
        revision = self.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name, self.writer, self.context)
            client_info.query_kind = QueryKind.INITIAL_QUERY
            await client_info.write(revision)

        settings_as_strings = (
            revision >= constants.DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS
        )
        await write_settings(
            self.writer, self.context.settings, settings_as_strings, self.settings_is_important
        )
        if revision >= constants.DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET:
            await self.writer.write_str("")
        await self.writer.write_varint(QueryProcessingStage.COMPLETE)
        await self.writer.write_varint(self.compression)

        await self.writer.write_str(
            query,
        )

        logger.debug("Query: %s", query)

        await self.writer.flush()

    async def _init_connection(self, host: str, port: int):
        self.host, self.port = host, port
        reader, writer = await asyncio.open_connection(host, port, ssl=self._get_ssl_context())
        self.writer = BufferedWriter(writer)
        self.reader = BufferedReader(reader)
        self.block_reader = self.get_block_reader()
        self.block_reader_raw = BlockReader(self.reader, self.writer, self.context)
        self.block_writer = self.get_block_writer()

        self.connected = True
        await self.send_hello()
        await self.receive_hello()

    def reset_state(self):

        self.writer = None
        self.reader = None
        self.block_reader = None
        self.block_reader_raw = None
        self.block_writer = None
        self.connected = False

        self.client_trace_context = None
        self.server_info = None

        self.is_query_executing = False

    async def disconnect(self):
        if self.connected:
            self.connected = False
            await self.writer.close()

        self.reset_state()

    async def connect(self):
        if self.connected:
            await self.disconnect()
        logger.debug("Connecting. Database: %s. User: %s", self.database, self.user)
        for host, port in self.hosts:
            logger.debug("Connecting to %s:%s", host, port)
            return await self._init_connection(host, port)

    async def execute(
        self,
        query,
        args=None,
        with_column_types=False,
        external_tables=None,
        query_id="",
        settings=None,
        types_check=False,
        columnar=False,
    ):
        """
        Executes query.

        Establishes new connection if it wasn't established yet.
        After query execution connection remains intact for next queries.
        If connection can't be reused it will be closed and new connection will
        be created.

        :param query: query that will be send to server.
        :param args: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result of the SELECT query will be
                         returned in column-oriented form.
                         It also allows to INSERT data in columnar form.
                         Defaults to ``False`` (row-like form).

        :return: * number of inserted rows for INSERT queries with data.
                   Returning rows count from INSERT FROM SELECT is not
                   supported.
                 * if `with_column_types=False`: `list` of `tuples` with
                   rows/columns.
                 * if `with_column_types=True`: `tuple` of 2 elements:
                    * The first element is `list` of `tuples` with
                      rows/columns.
                    * The second element information is about columns: names
                      and types.
        """

        start_time = time()
        async with ExecuteContext(self, query, settings):

            # INSERT queries can use list/tuple/generator of list/tuples/dicts.
            # For SELECT parameters can be passed in only in dict right now.
            is_insert = isinstance(args, (list, tuple, GeneratorType))

            if is_insert:
                rv = await self.process_insert_query(
                    query,
                    args,
                    external_tables=external_tables,
                    query_id=query_id,
                    types_check=types_check,
                    columnar=columnar,
                )
            else:
                rv = await self.process_ordinary_query(
                    query,
                    args,
                    with_column_types=with_column_types,
                    external_tables=external_tables,
                    query_id=query_id,
                    types_check=types_check,
                    columnar=columnar,
                )
            self.last_query.store_elapsed(time() - start_time)
            return rv

    async def execute_with_progress(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        settings=None,
        types_check=False,
        columnar=False,
    ):
        """
        Executes SELECT query with progress information.
        See, :ref:`execute-with-progress`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :param columnar: if specified the result will be returned in
                         column-oriented form.
                         Defaults to ``False`` (row-like form).
        :return: :ref:`progress-query-result` proxy.
        """

        async with ExecuteContext(self, query, settings):
            return await self.process_ordinary_query_with_progress(
                query,
                params=params,
                with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id,
                types_check=types_check,
                columnar=columnar,
            )

    async def process_ordinary_query_with_progress(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        types_check=False,
        columnar=False,
    ):

        if params is not None:
            query = self.substitute_params(query, params)

        await self.send_query(query, query_id=query_id)
        await self.send_external_tables(external_tables, types_check=types_check)
        return self.receive_result(
            with_column_types=with_column_types, progress=True, columnar=columnar
        )

    def make_query_settings(self, settings):
        settings = dict(settings or {})

        # Pick client-related settings.
        client_settings = self.client_settings.copy()
        for key in self.available_client_settings:
            if key in settings:
                client_settings[key] = settings.pop(key)

        self.context.client_settings = client_settings

        # The rest of settings are ClickHouse-related.
        query_settings = self.settings.copy()
        query_settings.update(settings)
        self.context.settings = query_settings

    async def check_query_execution(self):
        async with self._lock:
            if self.is_query_executing:
                raise PartiallyConsumedQueryError()

            self.is_query_executing = True

    async def force_connect(self):
        await self.check_query_execution()

        if not self.connected:
            await self.connect()

        elif not await self.ping():
            logger.warning("Connection was closed, reconnecting.")
            await self.connect()

    async def process_ordinary_query(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id="",
        types_check=False,
        columnar=False,
    ):

        if params is not None:
            query = self.substitute_params(query, params)

        await self.send_query(query, query_id=query_id)
        await self.send_external_tables(external_tables, types_check=types_check)
        return await self.receive_result(with_column_types=with_column_types, columnar=columnar)

    async def send_external_tables(self, tables, types_check=False):
        for table in tables or []:
            block = RowOrientedBlock(table["structure"], table["data"], types_check=types_check)
            await self.send_block(block, table_name=table["name"])

        # Empty block, end of data transfer.
        await self.send_block(RowOrientedBlock())

    async def send_block(self, block, table_name=""):
        await self.writer.write_varint(ClientPacket.DATA)

        revision = self.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            await self.writer.write_str(
                table_name,
            )

        await self.block_writer.write(block)

    def substitute_params(self, query, params):
        if not isinstance(params, dict):
            raise ValueError("Parameters are expected in dict form")

        escaped = escape_params(params)
        return query % escaped

    async def process_insert_query(
        self,
        query_without_data,
        data,
        external_tables=None,
        query_id=None,
        types_check=False,
        columnar=False,
    ):
        await self.send_query(query_without_data, query_id=query_id)
        await self.send_external_tables(external_tables, types_check=types_check)

        sample_block = await self.receive_sample_block()
        if sample_block:
            rv = await self.send_data(
                sample_block, data, types_check=types_check, columnar=columnar
            )
            packet = await self._receive_packet()
            if packet.exception:
                raise packet.exception
            return rv

    async def receive_sample_block(self):
        while True:
            packet = await self._receive_packet()

            if packet.type == ServerPacket.DATA:
                return packet.block

            elif packet.type == ServerPacket.EXCEPTION:
                raise packet.exception
            elif packet.type == ServerPacket.LOG:
                self.log_block(packet.block)
            elif packet.type == ServerPacket.TABLE_COLUMNS:
                pass

            else:
                message = self.unexpected_packet_message(
                    "Data, Exception or TableColumns", packet.type
                )
                raise UnexpectedPacketFromServerError(message)

    async def send_data(self, sample_block, data, types_check=False, columnar=False):
        inserted_rows = 0

        client_settings = self.context.client_settings
        block_cls = ColumnOrientedBlock if columnar else RowOrientedBlock
        slicer = column_chunks if columnar else chunks

        for chunk in slicer(data, client_settings["insert_block_size"]):
            block = block_cls(
                columns_with_types=sample_block.columns_with_types,
                data=chunk,
                types_check=types_check,
            )
            await self.send_block(block)
            inserted_rows += block.num_rows

        # Empty block means end of data.
        await self.send_block(block_cls())
        return inserted_rows

    async def iter_process_ordinary_query(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        types_check=False,
    ):

        if params is not None:
            query = self.substitute_params(query, params)

        await self.send_query(query, query_id=query_id)
        await self.send_external_tables(external_tables, types_check=types_check)
        return self.iter_receive_result(with_column_types=with_column_types)

    def iter_receive_result(self, with_column_types=False):
        gen = self.packet_generator()

        for rows in IterQueryResult(gen, with_column_types=with_column_types):
            for row in rows:
                yield row

    def track_current_database(self, query):
        query = query.strip("; ")
        if query.lower().startswith("use "):
            self.database = query[4:].strip()

    async def execute_iter(
        self,
        query,
        params=None,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        settings=None,
        types_check=False,
    ):
        """
        *New in version 0.0.14.*

        Executes SELECT query with results streaming. See, :ref:`execute-iter`.

        :param query: query that will be send to server.
        :param params: substitution parameters for SELECT queries and data for
                       INSERT queries. Data for INSERT can be `list`, `tuple`
                       or :data:`~types.GeneratorType`.
                       Defaults to ``None`` (no parameters  or data).
        :param with_column_types: if specified column names and types will be
                                  returned alongside with result.
                                  Defaults to ``False``.
        :param external_tables: external tables to send.
                                Defaults to ``None`` (no external tables).
        :param query_id: the query identifier. If no query id specified
                         ClickHouse server will generate it.
        :param settings: dictionary of query settings.
                         Defaults to ``None`` (no additional settings).
        :param types_check: enables type checking of data for INSERT queries.
                            Causes additional overhead. Defaults to ``False``.
        :return: :ref:`iter-query-result` proxy.
        """

        async with ExecuteContext(self, query, settings):

            return await self.iter_process_ordinary_query(
                query,
                params=params,
                with_column_types=with_column_types,
                external_tables=external_tables,
                query_id=query_id,
                types_check=types_check,
            )

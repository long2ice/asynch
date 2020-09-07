import getpass
import socket

from asynch.errors import LogicalError
from asynch.proto import constants
from asynch.proto.io import BufferedWriter


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


class Interface:
    TCP = 1
    HTTP = 2


class QueryKind:
    # Uninitialized object.
    NO_QUERY = 0

    INITIAL_QUERY = 1

    # Query that was initiated by another query for distributed query
    # execution.
    SECONDARY_QUERY = 2


class ClientInfo:
    client_version_major = constants.CLIENT_VERSION_MAJOR
    client_version_minor = constants.CLIENT_VERSION_MINOR
    client_version_patch = constants.CLIENT_VERSION_PATCH
    client_revision = constants.CLIENT_REVISION
    interface = Interface.TCP

    initial_user = ""
    initial_query_id = ""
    initial_address = "0.0.0.0:0"

    quota_key = ""

    def __init__(self, client_name: str):
        self.query_kind = QueryKind.NO_QUERY

        try:
            self.os_user = getpass.getuser()
        except KeyError:
            self.os_user = ""
        self.client_hostname = socket.gethostname()
        self.client_name = client_name

    @property
    def empty(self):
        return self.query_kind == QueryKind.NO_QUERY

    async def write(self, server_revision: int, writer: BufferedWriter):
        revision = server_revision
        if server_revision < constants.DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            raise LogicalError(
                "Method ClientInfo.write is called " "for unsupported server revision"
            )

        await writer.write_int8(self.query_kind,)
        if self.empty:
            return

        await writer.write_str(self.initial_user,)
        await writer.write_str(self.initial_query_id,)
        await writer.write_str(self.initial_address,)

        await writer.write_uint8(self.interface,)

        await writer.write_str(self.os_user,)
        await writer.write_str(self.client_hostname,)
        await writer.write_str(self.client_name,)
        await writer.write_varint(self.client_version_major,)
        await writer.write_varint(self.client_version_minor,)
        await writer.write_varint(self.client_revision,)

        if revision >= constants.DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO:
            await writer.write_str(self.quota_key,)

        if revision >= constants.DBMS_MIN_REVISION_WITH_VERSION_PATCH:
            await writer.write_varint(self.client_version_patch,)

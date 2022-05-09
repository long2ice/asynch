from typing import TYPE_CHECKING, Optional

from asynch.proto.result import QueryInfo

if TYPE_CHECKING:
    from asynch.proto.connection import Connection
    from asynch.proto.cs import ServerInfo


class Context:
    def __init__(self):
        self._server_info: Optional["ServerInfo"] = None
        self._settings = {}
        self._client_settings = {}

    @property
    def server_info(self):
        return self._server_info

    @server_info.setter
    def server_info(self, value):
        self._server_info = value

    @property
    def settings(self):
        return self._settings.copy()

    @settings.setter
    def settings(self, value):
        self._settings = value.copy()

    @property
    def client_settings(self):
        return self._client_settings.copy()

    @client_settings.setter
    def client_settings(self, value):
        self._client_settings = value.copy()


class ExecuteContext:
    def __init__(self, connection: "Connection", query, settings):
        self._query = query
        self._settings = settings
        self._connection = connection
        self._connection.make_query_settings(settings)

    async def __aenter__(self):
        await self._connection.force_connect()
        self._connection.last_query = QueryInfo(self._connection.reader)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            if exc_type in [Exception, KeyboardInterrupt]:
                await self._connection.disconnect()
                raise exc_val
        self._connection.track_current_database(self._query)

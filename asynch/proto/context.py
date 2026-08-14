from __future__ import annotations

from typing import TYPE_CHECKING

from asynch.errors import ServerException
from asynch.proto.result import QueryInfo

if TYPE_CHECKING:
    from asynch.proto.connection import Connection
    from asynch.proto.cs import ServerInfo


class Context:
    def __init__(self) -> None:
        self._server_info: ServerInfo | None = None
        self._settings: dict = {}
        self._client_settings: dict = {}

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
    def __init__(self, connection: Connection, query, settings):
        self._query = query
        self._settings = settings
        self._connection = connection
        self._connection.make_query_settings(settings)

    async def __aenter__(self):
        try:
            await self._connection.force_connect()
            self._connection.last_query = QueryInfo(self._connection.reader)
        except BaseException:
            await self._connection.disconnect()
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            if issubclass(exc_type, ServerException):
                # The server rejected the query but the connection itself is
                # fine (the exception packet leaves the stream at a boundary).
                # Dropping it here would make every SQL error cost a pooled
                # connection. The query failed, so the database is NOT tracked:
                # a failed `USE db` must not move the client's idea of it.
                raise exc_val
            # Any other failure leaves the query half-read, so the connection
            # has to go. This must catch BaseException, not Exception:
            # asyncio.CancelledError is a BaseException, and skipping it here
            # left `is_query_executing` set forever, so every later query on
            # that connection failed with "some records have not been
            # fetched". Cancelling a query - an `asyncio.timeout`, or a web
            # framework cancelling a request task - is routine.
            await self._connection.disconnect()
            raise exc_val
        self._connection.track_current_database(self._query)

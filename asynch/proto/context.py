from typing import Optional

from asynch.proto.cs import ServerInfo


class Context:
    def __init__(self):
        self._server_info: Optional[ServerInfo] = None
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

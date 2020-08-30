from asynch import Connection


class Cursor:
    def __init__(
        self,
        connection: Connection,
        with_column_types=False,
        external_tables=None,
        query_id=None,
        settings=None,
        types_check=False,
        columnar=False,
        stream_results: bool = False,
        max_row_buffer=0,
    ):
        self.stream_results = stream_results
        self.max_row_buffer = max_row_buffer
        self.settings = settings
        self.columnar = columnar
        self.query_id = query_id
        self.types_check = types_check
        self.external_tables = external_tables
        self.with_column_types = with_column_types
        self.connection = connection

    async def close(self):
        pass

    async def execute(
        self, query: str, args=None,
    ):
        pass

    async def executemany(self):
        pass

    async def fetchone(self):
        pass

    async def fetchmany(self, size: int):
        pass

    async def fetchall(self,):
        pass

    def _prepare(self):
        external_tables = [
            {"name": name, "structure": structure, "data": data}
            for name, (structure, data) in self.external_tables.items()
        ] or None

        execute = self.connection.execute

        if self.stream_results:
            execute = self.connection.execute_iter
            self.settings = self.settings or {}
            self.settings["max_block_size"] = self.max_row_buffer

        execute_kwargs = {
            "settings": self.settings,
            "external_tables": external_tables,
            "types_check": self.types_check,
            "query_id": self.query_id,
        }

        return execute, execute_kwargs


class DictCursor(Cursor):
    pass

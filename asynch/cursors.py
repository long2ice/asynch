import logging
from collections import namedtuple
from typing import Optional
from warnings import warn

from asynch.errors import InterfaceError, ProgrammingError
from asynch.proto.models.enums import CursorStatus

Column = namedtuple("Column", "name type_code display_size internal_size precision scale null_ok")

logger = logging.getLogger(__name__)


class States:
    warn(
        (
            "Should not be used in the version 0.2.6 or later."
            "Should be replaced with the reconsidered `CursorStatus` enum "
            "from the `asynch.proto.models.enums` module."
        ),
        DeprecationWarning,
    )
    (NONE, RUNNING, FINISHED, CURSOR_CLOSED) = range(4)


class Cursor:
    _columns_with_types = None

    def __init__(self, connection=None, echo: bool = False):
        self._connection = connection
        self._reset_state()
        self._rows = []
        self._echo = echo
        self._arraysize = 1

    def __repr__(self) -> str:
        cls_name = self.__class__.__name__
        status = self.status
        return (
            f"<{cls_name}(connection={self._connection}, echo={self._echo})"
            f" object at 0x{id(self):x}; status: {status}>"
        )

    @property
    def connection(self):
        """This read-only attribute return a reference to the Connection
        object on which the cursor was created."""
        return self._connection

    @property
    def rowcount(self):
        """
        :return: the number of rows that the last .execute*() produced.
        """
        return self._rowcount

    @property
    def status(self) -> str:
        """Return the status of the cursor.

        :return: the Cursor object status
        :rtype: str (CursorStatus StrEnum)
        """

        return self._state

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    async def close(self):
        self._state = CursorStatus.closed

    async def execute(
        self,
        query: str,
        args=None,
        context=None,
    ):
        self._check_cursor_closed()
        self._check_query_executing()
        self._begin_query()

        execute, execute_kwargs = self._prepare(context)

        response = await execute(query, args=args, with_column_types=True, **execute_kwargs)

        await self._process_response(response)
        self._end_query()
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    async def _process_response(self, response, executemany=False):
        if executemany or isinstance(response, int):
            self._rowcount = response
            response = None

        if not response:
            self._columns = self._types = self._rows = []
            return

        if self._stream_results:
            columns_with_types = await response.get_columns_with_types()
            rows = response

        else:
            rows, columns_with_types = response
        self._columns_with_types = columns_with_types
        if columns_with_types:
            self._columns, self._types = zip(*columns_with_types)
            if not self._stream_results:
                self._rowcount = len(rows)
        else:
            self._columns = self._types = []

        self._rows = rows

    async def executemany(self, query, args=None, context=None):
        self._check_cursor_closed()
        self._check_query_executing()
        self._begin_query()

        execute, execute_kwargs = self._prepare(context)

        response = await execute(query, args=args, **execute_kwargs)

        await self._process_response(response, executemany=True)
        self._end_query()
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    async def fetchone(self):
        self._check_query_started()

        if self._stream_results:
            try:
                return await self._rows.next()
            except Exception:
                return None

        if not self._rows:
            return None
        return self._rows.pop(0)

    async def fetchmany(self, size: Optional[int]):
        self._check_query_started()

        if size is None:
            size = self._arraysize
        if size == 0:
            return []

        if self._stream_results:
            rv = []
            async for i in self._rows:
                rv.append(i)
                if size > 0 and len(rv) >= size:
                    break
            return rv

        if size < 0:
            rv = self._rows
            self._rows = []
        else:
            rv = self._rows[:size]
            self._rows = self._rows[size:]
        return rv

    async def fetchall(self):
        self._check_query_started()

        if self._stream_results:
            return [row async for row in self._rows]

        rv = self._rows
        self._rows = []
        return rv

    def _reset_state(self):
        """Reset the state of the cursor.

        Prepares a cursor object to handle another query.
        """

        self._state = CursorStatus.ready

        self._columns = None
        self._types = None
        self._rows = None
        self._rowcount = -1

        self._stream_results = False
        self._max_row_buffer = 0
        self._settings = None
        self._query_id = ""
        self._external_tables = {}
        self._types_check = False

    def _make_external_tables(self):
        tables = []
        for name, (structure, data) in self._external_tables.items():
            tables.append({"name": name, "structure": structure, "data": data})

        return tables

    def make_external_tables(self, dialect, execution_options):
        external_tables = execution_options.get("external_tables", [])

        tables = []
        type_compiler = dialect.type_compiler

        for table in external_tables:
            structure = []
            for c in table.columns:
                type_ = type_compiler.process(c.type, type_expression=c)
                structure.append((c.name, type_))

            tables.append(
                {
                    "name": table.name,
                    "structure": structure,
                    "data": table.dialect_options["clickhouse"]["data"],
                }
            )

        # for backward compatibility
        tables.extend(self._make_external_tables())

        return tables

    def _prepare(self, context=None):
        if context:
            execution_options = context.execution_options

            external_tables = self.make_external_tables(context.dialect, execution_options)
        else:
            execution_options = {}
            external_tables = self._make_external_tables() or None

        execute = self._connection._connection.execute  # noqa
        settings = execution_options.get("settings", self._settings or {})
        self._stream_results = execution_options.get("stream_results", self._stream_results)

        if self._stream_results:
            execute = self._connection._connection.execute_iter  # noqa
            self._max_row_buffer = execution_options.get("max_block_size", self._max_row_buffer)
            settings["max_block_size"] = self._max_row_buffer

        self._settings = settings

        execute_kwargs = {
            "settings": settings,
            "external_tables": external_tables,
            "types_check": execution_options.get("types_check", self._types_check),
            "query_id": self._query_id,
        }

        return execute, execute_kwargs

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            one = await self.fetchone()
            if not one:
                raise StopAsyncIteration
            return one

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __aenter__(self):
        return self

    @property
    def description(self):
        if self._state == CursorStatus.ready:
            return None

        columns = self._columns or []
        types = self._types or []

        return [
            Column(name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    def _check_query_started(self):
        if self._state == CursorStatus.ready:
            raise ProgrammingError(f"no results to fetch from the {self}")

    def _check_query_executing(self):
        if self._connection._connection.is_query_executing:
            raise ProgrammingError(
                "some records have not been fetched. fetch the remaining records before executing the next query"
            )

    def _check_cursor_closed(self):
        if self._state == CursorStatus.closed:
            raise InterfaceError(f"the {self} is already closed")

    def _begin_query(self):
        self._state = CursorStatus.running

    def _end_query(self):
        self._state = CursorStatus.finished

    def set_stream_results(self, stream_results, max_row_buffer):
        """
        Toggles results streaming from server. Driver will consume
        block-by-block of `max_row_buffer` size and yield row-by-row from each
        block.

        :param stream_results: enable or disable results streaming.
        :param max_row_buffer: specifies the maximum number of rows to buffer
               at a time.
        :return: None
        """
        self._stream_results = stream_results
        self._max_row_buffer = max_row_buffer

    def set_settings(self, settings):
        """
        Specifies settings for cursor.

        :param settings: dictionary of query settings
        :return: None
        """
        self._settings = settings

    def set_types_check(self, types_check):
        """
        Toggles type checking for sequence of INSERT parameters.
        Disabled by default.

        :param types_check: new types check value.
        :return: None
        """
        self._types_check = types_check

    def set_external_table(self, name, structure, data):
        """
        Adds external table to cursor context.

        If the same table is specified more than once the last one is used.

        :param name: name of external table
        :param structure: list of tuples (name, type) that defines table
                          structure. Example [(x, 'Int32')].
        :param data: sequence of rows of tuples or dicts for transmission.
        :return: None
        """
        self._external_tables[name] = (structure, data)

    def set_query_id(self, query_id=""):
        """
        Specifies the query identifier for cursor.

        :param query_id: the query identifier.
        :return: None
        """
        self._query_id = query_id

    # End non-PEP methods


class DictCursor(Cursor):
    async def fetchone(self) -> dict:
        """Fetch exactly one row from the last executed query.

        :raises AttributeError: columns mismatch

        :return: one row from the query
        :rtype: dict
        """

        row = await super().fetchone()
        if self._columns:
            return dict(zip(self._columns, row)) if row else {}
        raise AttributeError("Invalid columns.")

    async def fetchmany(self, size: int) -> list[dict]:
        """Fetch no more than `size` rows from the last executed query.

        :raises AttributeError: columns mismatch

        :return: the list of rows from the query
        :rtype: list[dict]
        """

        rows = await super().fetchmany(size)
        if self._columns:
            return [dict(zip(self._columns, item)) for item in rows] if rows else []
        raise AttributeError("Invalid columns.")

    async def fetchall(self) -> list[dict]:
        """Fetch all resulting rows from the last executed query.

        :raises AttributeError: columns mismatch

        :return: the list of all possible rows from the query
        :rtype: list[dict]
        """

        rows = await super().fetchall()
        if self._columns:
            return [dict(zip(self._columns, item)) for item in rows] if rows else []
        raise AttributeError("Invalid columns.")

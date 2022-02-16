import logging
from collections import namedtuple
from itertools import islice

from asynch.errors import InterfaceError, ProgrammingError

Column = namedtuple("Column", "name type_code display_size internal_size precision scale null_ok")

logger = logging.getLogger(__name__)


class States:
    (NONE, RUNNING, FINISHED, CURSOR_CLOSED) = range(4)


class Cursor:
    _states = States()
    _columns_with_types = None

    def __init__(self, connection=None, echo=False):
        self._connection = connection
        self._reset_state()
        self._rows = []
        self._echo = echo
        self._arraysize = 1

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

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    async def close(self):
        conn = self._connection
        if conn is None:
            return
        await self._connection.close()
        self._connection = None

    async def execute(
        self,
        query: str,
        args=None,
    ):
        self._check_cursor_closed()
        self._begin_query()

        execute, execute_kwargs = self._prepare()

        response = await execute(query, args=args, with_column_types=True, **execute_kwargs)

        self._process_response(response)
        self._end_query()
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    def _process_response(self, response, executemany=False):
        if executemany or isinstance(response, int):
            self._rowcount = response
            response = None

        if not response:
            self._columns = self._types = self._rows = []
            return

        if self._stream_results:
            columns_with_types = next(response)
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

    async def executemany(self, query, args=None):
        self._check_cursor_closed()
        self._begin_query()

        execute, execute_kwargs = self._prepare()

        response = await execute(query, args=args, **execute_kwargs)

        self._process_response(response, executemany=True)
        self._end_query()
        if self._echo:
            logger.info(query)
            logger.info("%r", args)
        return self._rowcount

    def fetchone(self):
        self._check_query_started()

        if self._stream_results:
            return next(self._rows, None)

        else:
            if not self._rows:
                return None

            return self._rows.pop(0)

    def fetchmany(self, size: int):
        self._check_query_started()

        if size is None:
            size = self._arraysize

        if self._stream_results:
            if size == -1:
                return list(self._rows)
            else:
                return list(islice(self._rows, size))

        if size < 0:
            rv = self._rows
            self._rows = []
        else:
            rv = self._rows[:size]
            self._rows = self._rows[size:]

        return rv

    def fetchall(
        self,
    ):
        self._check_query_started()

        if self._stream_results:
            return list(self._rows)

        rv = self._rows
        self._rows = []
        return rv

    def _reset_state(self):
        """
        Resets query state and get ready for another query.
        """
        self._state = self._states.NONE

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

    def _prepare(self):
        external_tables = [
            {"name": name, "structure": structure, "data": data}
            for name, (structure, data) in self._external_tables.items()
        ] or None

        execute = self._connection._connection.execute

        if self._stream_results:
            execute = self._connection._connection.execute_iter
            self.settings = self.settings or {}
            self.settings["max_block_size"] = self._max_row_buffer

        execute_kwargs = {
            "settings": self._settings,
            "external_tables": external_tables,
            "types_check": self._types_check,
            "query_id": self._query_id,
        }

        return execute, execute_kwargs

    def __iter__(self):
        while True:
            one = self.fetchone()
            if one is None:
                return
            yield one

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __aenter__(self):
        return self

    @property
    def description(self):
        if self._state == self._states.NONE:
            return None

        columns = self._columns or []
        types = self._types or []

        return [
            Column(name, type_code, None, None, None, None, True)
            for name, type_code in zip(columns, types)
        ]

    def _check_query_started(self):
        if self._state == self._states.NONE:
            raise ProgrammingError("no results to fetch")

    def _check_cursor_closed(self):
        if self._state == self._states.CURSOR_CLOSED:
            raise InterfaceError("cursor already closed")

    def _begin_query(self):
        self._state = self._states.RUNNING

    def _end_query(self):
        self._state = self._states.FINISHED

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
    def _process_response(self, response, executemany=False):
        super(DictCursor, self)._process_response(response, executemany)
        if self._columns and self._rows:
            self._rows = [dict(zip(self._columns, item)) for item in self._rows]

from typing import AsyncGenerator

from asynch.proto.block import BlockStreamProfileInfo
from asynch.proto.progress import Progress
from asynch.proto.streams.buffered import BufferedReader


class QueryResult:
    """
    Stores query result from multiple blocks.
    """

    def __init__(
        self,
        reader: BufferedReader,
        packet_generator: AsyncGenerator,
        with_column_types=False,
        columnar=False,
    ):
        self.reader = reader
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.data = []
        self.columns_with_types = []
        self.columnar = columnar

    def store(self, packet):
        block = getattr(packet, "block", None)
        if block is None:
            return

        # Header block contains no rows. Pick columns from it.
        if block.num_rows:
            if self.columnar:
                columns = block.get_columns()
                if self.data:
                    # Extend corresponding column.
                    for i, column in enumerate(columns):
                        self.data[i].extend(column)
                else:
                    # Cast tuples to lists for further extending.
                    # Concatenating tuples produce new tuple. It's slow.
                    self.data = [list(c) for c in columns]
            else:
                self.data.extend(block.get_rows())

        elif not self.columns_with_types:
            self.columns_with_types = block.columns_with_types

    async def get_result(self):
        """
        :return: stored query result.
        """

        async for packet in self.packet_generator:
            self.store(packet)

        data = self.data
        if self.columnar:
            data = [tuple(c) for c in self.data]

        if self.with_column_types:
            return data, self.columns_with_types
        else:
            return data


class ProgressQueryResult(QueryResult):
    """
    Stores query result and progress information from multiple blocks.
    Provides iteration over query progress.
    """

    def __init__(
        self, reader: BufferedReader, packet_generator, with_column_types=False, columnar=False
    ):
        super().__init__(reader, packet_generator, with_column_types, columnar)
        self.progress_totals = Progress(self.reader)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            packet = next(self.packet_generator)
            progress_packet = getattr(packet, "progress", None)
            if progress_packet:
                self.progress_totals.increment(progress_packet)
                return self.progress_totals.rows, self.progress_totals.total_rows
            else:
                self.store(packet)

    async def get_result(self):
        # Read all progress packets.
        for _ in self:
            pass

        return await super(ProgressQueryResult, self).get_result()


class IterQueryResult:
    """
    Provides iteration over returned data by chunks (streaming by chunks).
    """

    def __init__(self, packet_generator, with_column_types=False):
        self.packet_generator = packet_generator
        self.with_column_types = with_column_types

        self.first_block = True

    def __iter__(self):
        return self

    def next(self):
        packet = next(self.packet_generator)
        block = getattr(packet, "block", None)
        if block is None:
            return []

        if self.first_block and self.with_column_types:
            self.first_block = False
            rv = [block.columns_with_types]
            rv.extend(block.get_rows())
            return rv
        else:
            return block.get_rows()

    # For Python 3.
    __next__ = next


class QueryInfo:
    def __init__(self, reader: BufferedReader):
        self.profile_info = BlockStreamProfileInfo(reader)
        self.progress = Progress(reader)
        self.elapsed = 0

    def store_profile(self, profile_info):
        self.profile_info = profile_info

    def store_progress(self, progress):
        if self.progress:
            self.progress.increment(progress)
        else:
            self.progress = progress

    def store_elapsed(self, elapsed):
        self.elapsed = elapsed

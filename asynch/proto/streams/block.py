from asynch.proto import constants
from asynch.proto.block import BaseBlock, BlockInfo, ColumnOrientedBlock
from asynch.proto.columns import read_column, write_column
from asynch.proto.context import Context
from asynch.proto.streams.buffered import BufferedReader, BufferedWriter


class BlockWriter:
    def __init__(self, reader: BufferedReader, writer: BufferedWriter, context: Context):
        self.reader = reader
        self.writer = writer
        self.context = context

    async def write(self, block: BaseBlock):
        revision = self.context.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            await block.info.write(self.writer)

        # We write transposed data.
        n_columns = block.num_columns
        n_rows = block.num_rows

        await self.writer.write_varint(n_columns)
        await self.writer.write_varint(n_rows)

        for i, (col_name, col_type) in enumerate(block.columns_with_types):
            await self.writer.write_str(
                col_name,
            )
            await self.writer.write_str(
                col_type,
            )

            if n_columns:
                try:
                    items = block.get_column_by_index(i)
                except IndexError:
                    raise ValueError("Different rows length")

                await write_column(
                    self.reader,
                    self.writer,
                    self.context,
                    col_name,
                    col_type,
                    items,
                    types_check=block.types_check,
                )

        await self.finalize()

    async def finalize(self):
        await self.writer.flush()


class BlockReader:
    def __init__(self, reader: BufferedReader, writer: BufferedWriter, context):
        self.writer = writer
        self.reader = reader
        self.context = context

    async def read(self):
        info = BlockInfo()

        revision = self.context.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            await info.read(self.reader)

        n_columns = await self.reader.read_varint()
        n_rows = await self.reader.read_varint()

        data, names, types = [], [], []

        for i in range(n_columns):
            column_name = await self.reader.read_str()
            column_type = await self.reader.read_str()

            names.append(column_name)
            types.append(column_type)

            if n_rows:
                column = await read_column(
                    self.reader,
                    self.writer,
                    self.context,
                    column_type,
                    n_rows,
                )
                data.append(column)

        block = ColumnOrientedBlock(
            columns_with_types=list(zip(names, types)),
            data=data,
            info=info,
        )

        return block

from asynch.proto import constants
from asynch.proto.block import BaseBlock, BlockInfo
from asynch.proto.context import Context
from asynch.proto.io import BufferedWriter


class BlockOutputStream:
    def __init__(self, writer: BufferedWriter, context: Context):
        self.writer = writer
        self.context = context

    async def write(self, block: BaseBlock):
        revision = self.context.server_info.revision
        if revision >= constants.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            await block.info.write()

        # We write transposed data.
        n_columns = block.num_columns
        n_rows = block.num_rows

        await self.writer.write_varint(n_columns,)
        await self.writer.write_varint(n_rows)

        for i, (col_name, col_type) in enumerate(block.columns_with_types):
            await self.writer.write_str(col_name,)
            await self.writer.write_str(col_type,)

            if n_columns:
                try:
                    items = block.get_column_by_index(i)
                except IndexError:
                    raise ValueError("Different rows length")

                write_column(
                    self.context,
                    col_name,
                    col_type,
                    items,
                    self.fout,
                    types_check=block.types_check,
                )

        await self.finalize()

    async def finalize(self):
        await self.writer.flush()


class BlockInputStream:
    def __init__(self, fin, context):
        self.fin = fin
        self.context = context

        super(BlockInputStream, self).__init__()

    def read(self):
        info = BlockInfo()

        revision = self.context.server_info.revision
        if revision >= defines.DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            info.read(self.fin)

        n_columns = read_varint(self.fin)
        n_rows = read_varint(self.fin)

        data, names, types = [], [], []

        for i in range(n_columns):
            column_name = read_binary_str(self.fin)
            column_type = read_binary_str(self.fin)

            names.append(column_name)
            types.append(column_type)

            if n_rows:
                column = read_column(self.context, column_type, n_rows, self.fin)
                data.append(column)

        block = ColumnOrientedBlock(
            columns_with_types=list(zip(names, types)), data=data, info=info,
        )

        return block

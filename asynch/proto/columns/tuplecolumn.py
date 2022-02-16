from .base import Column


class TupleColumn(Column):
    py_types = (list, tuple)

    def __init__(self, nested_columns, **kwargs):
        self.nested_columns = nested_columns
        super(TupleColumn, self).__init__(**kwargs)

    async def write_data(
        self,
        items,
    ):
        if not items:
            return
        items = list(zip(*items))

        for i, x in enumerate(self.nested_columns):
            await x.write_data(
                list(items[i]),
            )

    async def write_items(
        self,
        items,
    ):
        return await self.write_data(
            items,
        )

    async def read_data(
        self,
        n_items,
    ):
        rv = [
            await x.read_data(
                n_items,
            )
            for x in self.nested_columns
        ]
        return list(zip(*rv))

    async def read_items(
        self,
        n_items,
    ):
        return await self.read_data(
            n_items,
        )


def create_tuple_column(spec, column_by_spec_getter, column_options):
    brackets = 0
    column_begin = 0

    inner_spec = get_inner_spec(spec)
    nested_columns = []
    for i, x in enumerate(inner_spec + ","):
        if x == ",":
            if brackets == 0:
                nested_columns.append(inner_spec[column_begin:i])
                column_begin = i + 1
        elif x == "(":
            brackets += 1
        elif x == ")":
            brackets -= 1
        elif x == " ":
            if brackets == 0:
                column_begin = i + 1

    return TupleColumn([column_by_spec_getter(x) for x in nested_columns], **column_options)


def get_inner_spec(spec):
    brackets = 1
    offset = len("Tuple(")
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if brackets == 0:
            break

        if ch == "(":
            brackets += 1

        elif ch == ")":
            brackets -= 1

    return spec[offset:i]

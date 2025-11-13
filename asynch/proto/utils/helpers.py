from itertools import islice, tee
from string import Formatter


def chunks(seq, n):
    it = iter(seq)
    item = list(islice(it, n))
    while item:
        yield item
        item = list(islice(it, n))


def column_chunks(columns, n):
    for column in columns:
        if not isinstance(column, (list, tuple)):
            raise TypeError(f"Unsupported column type: {type(column)}. list or tuple is expected.")

    # create chunk generator for every column
    g = [chunks(column, n) for column in columns]

    while True:
        # get next chunk for every column
        item = [next(column, []) for column in g]
        if not any(item):
            break
        yield item


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def query_is_format_style(query: str) -> bool:
    """Validate that query uses {name} style formatting"""
    return any(name for _, name, *_ in Formatter().parse(query) if name)

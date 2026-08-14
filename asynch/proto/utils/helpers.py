from itertools import islice, tee


def asbool(obj) -> bool:
    if isinstance(obj, str):
        obj = obj.strip().lower()
        if obj in ("true", "yes", "on", "y", "t", "1"):
            return True
        elif obj in ("false", "no", "off", "n", "f", "0"):
            return False
        else:
            raise ValueError(f"String is not true/false: {obj!r}")
    return bool(obj)


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

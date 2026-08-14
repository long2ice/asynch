# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
def get_inner_spec(column_name, spec):
    brackets = 0
    offset = len(column_name)
    i = offset
    for i, ch in enumerate(spec[offset:], offset):
        if ch == "(":
            brackets += 1

        elif ch == ")":
            brackets -= 1

        if brackets == 0:
            break

    return spec[offset + 1 : i]


def get_inner_columns(spec):
    brackets = 0
    column_begin = 0

    columns = []
    for i, x in enumerate(spec + ","):
        if x == ",":
            if brackets == 0:
                columns.append(spec[column_begin:i])
                column_begin = i + 1
        elif x == "(":
            brackets += 1
        elif x == ")":
            brackets -= 1
        elif x == " ":
            if brackets == 0:
                column_begin = i + 1
    return columns


def get_inner_columns_with_types(spec):
    brackets = 0
    prev_comma = 0
    prev_space = 0

    columns = []
    for i, x in enumerate(spec + ","):
        if x == ",":
            if brackets == 0:
                columns.append((spec[prev_comma:prev_space].strip(), spec[prev_space:i]))
                prev_comma = i + 1
        elif x == "(":
            brackets += 1
        elif x == ")":
            brackets -= 1
        elif x == " ":
            if brackets == 0:
                prev_space = i + 1
    return columns

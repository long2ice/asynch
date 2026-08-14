# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from .arraycolumn import create_array_column
from .util import get_inner_columns, get_inner_columns_with_types, get_inner_spec


def create_nested_column(spec, column_by_spec_getter, column_options):
    return create_array_column(
        "Array(Tuple({}))".format(",".join(get_nested_columns(spec))),
        column_by_spec_getter,
        column_options,
    )


def get_nested_columns(spec):
    inner_spec = get_inner_spec("Nested", spec)
    return get_inner_columns(inner_spec)


def get_columns_with_types(spec):
    inner_spec = get_inner_spec("Nested", spec)
    return get_inner_columns_with_types(inner_spec)

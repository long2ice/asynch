# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
def create_nullable_column(spec, column_by_spec_getter):
    inner = spec[9:-1]
    nested = column_by_spec_getter(inner)
    nested.nullable = True
    return nested

# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
def create_simple_aggregate_function_column(spec, column_by_spec_getter):
    # SimpleAggregateFunction(Func, Type) -> Type
    # We allow 1 split, because the nested Type can contain more comma.
    inner = spec[24:-1].split(",", 1)[1].strip()
    nested = column_by_spec_getter(inner)
    return nested

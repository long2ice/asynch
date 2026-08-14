# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from asynch.proto.columns.base import FormatColumn


class BoolColumn(FormatColumn):
    ch_type = "Bool"
    py_types = (bool,)
    format = "?"

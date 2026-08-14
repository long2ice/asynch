# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from .intcolumn import FormatColumn


# TODO: Drop Null column support in future.
# Compatibility with old servers.
class NullColumn(FormatColumn):
    ch_type = "Null"
    format = "B"

    @property
    def size(self):
        return 1

    def after_read_items(self, items, nulls_map=None):
        return (None,) * len(items)

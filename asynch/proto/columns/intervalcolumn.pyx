# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from .intcolumn import Int64Column


class IntervalColumn(Int64Column):
    pass


class IntervalDayColumn(IntervalColumn):
    ch_type = "IntervalDay"


class IntervalWeekColumn(IntervalColumn):
    ch_type = "IntervalWeek"


class IntervalMonthColumn(IntervalColumn):
    ch_type = "IntervalMonth"


class IntervalYearColumn(IntervalColumn):
    ch_type = "IntervalYear"


class IntervalHourColumn(IntervalColumn):
    ch_type = "IntervalHour"


class IntervalMinuteColumn(IntervalColumn):
    ch_type = "IntervalMinute"


class IntervalSecondColumn(IntervalColumn):
    ch_type = "IntervalSecond"

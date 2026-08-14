# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
from calendar import timegm
from datetime import datetime
from datetime import timezone as datetime_timezone
from time import mktime
from zoneinfo import ZoneInfo

from cpython.datetime cimport datetime_new, import_datetime

from tzlocal import get_localzone

from .base import FormatColumn

import_datetime()

utc = datetime_timezone.utc

# Sentinel for "this minute bucket straddles a UTC-offset change".
cdef long long _MIXED_BUCKET = -1000000000


def get_timezone(tz_name):
    return ZoneInfo(tz_name)


cdef inline long long _floordiv(long long a, long long b) noexcept:
    # C division truncates toward zero; floor toward -inf instead
    # (needed for pre-1970 DateTime64 values).
    cdef long long q = a // b
    if a % b != 0 and (a < 0) != (b < 0):
        q -= 1
    return q


cdef inline void _civil_from_days(long long z, int* year, int* month, int* day) noexcept:
    # Howard Hinnant's civil_from_days; written for C truncating division.
    cdef long long era, doe, yoe, doy, mp
    z += 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    day[0] = <int> (doy - (153 * mp + 2) // 5 + 1)
    month[0] = <int> (mp + 3 if mp < 10 else mp - 9)
    year[0] = <int> (yoe + era * 400 + (1 if month[0] <= 2 else 0))


def _build_naive_datetimes(items, tz, nulls_map):
    """tuple of naive datetimes for epoch seconds `items`, converted via `tz`.

    Replaces per-value `fromtimestamp(ts, tz).replace(tzinfo=None)`: the UTC
    offset is probed once per 24h bucket (both bucket ends, so a bucket
    straddling a DST transition falls back to the exact per-value path for
    that whole bucket) and the wall time is then built with C civil-date
    math. Day-sized buckets keep the cache effective even for timestamps
    spread over decades.
    """
    cdef long long ts, shifted, days, secs, bucket
    cdef long long off
    cdef int y, m, d
    cdef list out = []

    fromts = datetime.fromtimestamp
    cdef dict cache = {}

    iterable = items if nulls_map is None else zip(items, nulls_map)
    for entry in iterable:
        if nulls_map is None:
            item = entry
        else:
            item, is_null = entry
            if is_null:
                out.append(None)
                continue
        ts = item
        bucket = _floordiv(ts, 86400)
        cached = cache.get(bucket)
        if cached is None:
            start = bucket * 86400
            off1 = int(fromts(start, tz).utcoffset().total_seconds())
            off2 = int(fromts(start + 86399, tz).utcoffset().total_seconds())
            off = off1 if off1 == off2 else _MIXED_BUCKET
            cache[bucket] = off
        else:
            off = cached
        if off == _MIXED_BUCKET:
            out.append(fromts(ts, tz).replace(tzinfo=None))
            continue
        shifted = ts + off
        days = _floordiv(shifted, 86400)
        secs = shifted - days * 86400
        _civil_from_days(days, &y, &m, &d)
        out.append(
            datetime_new(
                y, m, d, <int> (secs // 3600), <int> ((secs % 3600) // 60), <int> (secs % 60), 0, None
            )
        )
    return tuple(out)


def _build_naive_datetimes64(items, tz, nulls_map, long long ticks,
                             long long usec_mul, long long usec_div):
    """DateTime64 counterpart of `_build_naive_datetimes`.

    `items` are integer ticks at 10**scale_exp per second. Exact integer math
    (the old path divided by a float, which could round the last microsecond
    digit for far-future dates); sub-microsecond precision truncates to
    microseconds like `fromtimestamp` did.
    """
    cdef long long ts, frac, shifted, days, secs, bucket
    cdef long long off, usec
    cdef int y, m, d
    cdef list out = []

    fromts = datetime.fromtimestamp
    cdef dict cache = {}

    iterable = items if nulls_map is None else zip(items, nulls_map)
    for entry in iterable:
        if nulls_map is None:
            item = entry
        else:
            item, is_null = entry
            if is_null:
                out.append(None)
                continue
        ts = _floordiv(item, ticks)
        frac = item - ts * ticks
        usec = frac * usec_mul // usec_div
        bucket = _floordiv(ts, 86400)
        cached = cache.get(bucket)
        if cached is None:
            start = bucket * 86400
            off1 = int(fromts(start, tz).utcoffset().total_seconds())
            off2 = int(fromts(start + 86399, tz).utcoffset().total_seconds())
            off = off1 if off1 == off2 else _MIXED_BUCKET
            cache[bucket] = off
        else:
            off = cached
        if off == _MIXED_BUCKET:
            out.append(fromts(ts + frac / <double> ticks, tz).replace(tzinfo=None))
            continue
        shifted = ts + off
        days = _floordiv(shifted, 86400)
        secs = shifted - days * 86400
        _civil_from_days(days, &y, &m, &d)
        out.append(
            datetime_new(
                y,
                m,
                d,
                <int> (secs // 3600),
                <int> ((secs % 3600) // 60),
                <int> (secs % 60),
                <int> usec,
                None,
            )
        )
    return tuple(out)


def _localize(item, tz):
    # pytz timezones need .localize(); zoneinfo/stdlib tzinfo attach directly.
    # Support both so user-supplied pytz objects keep working.
    localize = getattr(tz, "localize", None)
    if localize is not None:
        return localize(item)
    return item.replace(tzinfo=tz)


class DateTimeColumn(FormatColumn):
    ch_type = "DateTime"
    py_types = (datetime, int)
    format = "I"

    def __init__(self, timezone=None, offset_naive=True, **kwargs):
        self.timezone = timezone
        self.offset_naive = offset_naive
        super().__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        tz = self.timezone
        fromts = datetime.fromtimestamp

        # A bit ugly copy-paste. But it helps save time on items
        # processing by avoiding lambda calls or if in loop.
        if self.offset_naive:
            if tz:
                # Works for any tzinfo (zoneinfo, pytz, fixed offsets): the
                # probe goes through aware fromtimestamp + utcoffset().
                return _build_naive_datetimes(items, tz, nulls_map)
            else:
                if nulls_map is None:
                    return tuple(fromts(item) for item in items)
                else:
                    return tuple(
                        (None if is_null else fromts(items[i]))
                        for i, is_null in enumerate(nulls_map)
                    )

        else:
            if nulls_map is None:
                return tuple(fromts(item, tz) for item in items)
            else:
                return tuple(
                    (None if is_null else fromts(items[i], tz))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        timezone = self.timezone
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if isinstance(item, int):
                # support supplying raw integers to avoid
                # costly timezone conversions when using datetime
                continue
            elif isinstance(item, str):
                # Lazy: ciso8601 has not declared free-threading support;
                # importing it at module level would re-enable the GIL for
                # the whole process on free-threaded CPython.
                import ciso8601

                item = ciso8601.parse_datetime(item)
            if timezone:
                # Set server's timezone for offset-naive datetime.
                if item.tzinfo is None:
                    item = _localize(item, timezone)

                item = item.astimezone(utc)
                items[i] = int(timegm(item.timetuple()))

            else:
                # If datetime is offset-aware use it's timezone.
                if item.tzinfo is not None:
                    item = item.astimezone(utc)
                    items[i] = int(timegm(item.timetuple()))

                else:
                    items[i] = int(mktime(item.timetuple()))


class DateTime64Column(DateTimeColumn):
    ch_type = "DateTime64"
    format = "q"  # signed: DateTime64 is an Int64 tick count, pre-1970 values are negative

    max_scale = 6

    def __init__(self, scale=0, **kwargs):
        self.scale = scale
        super().__init__(**kwargs)

    def after_read_items(self, items, nulls_map=None):
        scale = float(10**self.scale)

        tz = self.timezone
        fromts = datetime.fromtimestamp

        # A bit ugly copy-paste. But it helps save time on items
        # processing by avoiding lambda calls or if in loop.
        if self.offset_naive:
            if tz:
                return _build_naive_datetimes64(
                    items,
                    tz,
                    nulls_map,
                    10**self.scale,
                    10 ** (6 - self.scale) if self.scale <= 6 else 1,
                    10 ** (self.scale - 6) if self.scale > 6 else 1,
                )
            else:
                if nulls_map is None:
                    return tuple(fromts(item / scale) for item in items)
                else:
                    return tuple(
                        (None if is_null else fromts(items[i] / scale))
                        for i, is_null in enumerate(nulls_map)
                    )

        else:
            if nulls_map is None:
                return tuple(fromts(item / scale, tz) for item in items)
            else:
                return tuple(
                    (None if is_null else fromts(items[i] / scale, tz))
                    for i, is_null in enumerate(nulls_map)
                )

    def before_write_items(self, items, nulls_map=None):
        scale = 10**self.scale
        frac_scale = 10 ** (self.max_scale - self.scale)

        timezone = self.timezone
        null_value = self.null_value

        for i, item in enumerate(items):
            if nulls_map and nulls_map[i]:
                items[i] = null_value
                continue

            if isinstance(item, int):
                # support supplying raw integers to avoid
                # costly timezone conversions when using datetime
                continue

            if timezone:
                # Set server's timezone for offset-naive datetime.
                if item.tzinfo is None:
                    item = _localize(item, timezone)

                item = item.astimezone(utc)
                items[i] = int(timegm(item.timetuple())) * scale + int(
                    item.microsecond / frac_scale
                )

            else:
                # If datetime is offset-aware use it's timezone.
                if item.tzinfo is not None:
                    item = item.astimezone(utc)
                    items[i] = int(timegm(item.timetuple())) * scale + int(
                        item.microsecond / frac_scale
                    )

                else:
                    items[i] = int(mktime(item.timetuple())) * scale + int(
                        item.microsecond / frac_scale
                    )


def create_datetime_column(spec, column_options):
    if spec.startswith("DateTime64"):
        cls = DateTime64Column
        spec = spec[11:-1]
        params = spec.split(",", 1)
        column_options["scale"] = int(params[0])
        if len(params) > 1:
            spec = params[1].strip() + ")"
    else:
        cls = DateTimeColumn
        spec = spec[9:]

    context = column_options["context"]

    tz_name = timezone = None
    offset_naive = True

    # Use column's timezone if it's specified.
    if spec and spec[-1] == ")":
        tz_name = spec[1:-2]
        offset_naive = False
    else:
        if not context.settings.get("use_client_time_zone", False):
            try:
                local_timezone = str(get_localzone())
            except Exception:
                local_timezone = None

            if local_timezone != context.server_info.timezone:
                tz_name = context.server_info.timezone

    if tz_name:
        timezone = get_timezone(tz_name)

    return cls(timezone=timezone, offset_naive=offset_naive, **column_options)

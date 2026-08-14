"""DST-boundary conversions after the pytz → zoneinfo migration.

`before_write_items` turns naive datetimes into UTC epoch seconds using the
column timezone; these cases pin the fold/DST semantics that used to come from
`pytz.localize`.
"""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

from asynch.proto.columns.datetimecolumn import DateTime64Column, DateTimeColumn


def make_column(tz):
    return DateTimeColumn(timezone=tz, offset_naive=True, reader=None, writer=None)


@pytest.mark.parametrize(
    ("naive", "expected_utc"),
    [
        # Winter (CET, +01:00)
        (datetime(2024, 1, 15, 12, 0, 0), datetime(2024, 1, 15, 11, 0, 0)),
        # Summer (CEST, +02:00)
        (datetime(2024, 7, 15, 12, 0, 0), datetime(2024, 7, 15, 10, 0, 0)),
        # Right after the spring-forward gap (02:00→03:00 on 2024-03-31)
        (datetime(2024, 3, 31, 3, 30, 0), datetime(2024, 3, 31, 1, 30, 0)),
        # Ambiguous fall-back time (02:30 occurs twice on 2024-10-27);
        # fold=0 (the first occurrence, CEST) is the zoneinfo default.
        (datetime(2024, 10, 27, 2, 30, 0), datetime(2024, 10, 27, 0, 30, 0)),
    ],
)
def test_write_naive_datetime_across_dst(naive, expected_utc):
    column = make_column(ZoneInfo("Europe/Berlin"))
    items = [naive]
    column.before_write_items(items)
    assert items[0] == int(expected_utc.replace(tzinfo=timezone.utc).timestamp())


def test_write_accepts_pytz_timezone():
    pytz = pytest.importorskip("pytz")
    column = make_column(pytz.timezone("Europe/Berlin"))
    items = [datetime(2024, 1, 15, 12, 0, 0)]
    column.before_write_items(items)
    expected = datetime(2024, 1, 15, 11, 0, 0, tzinfo=timezone.utc)
    assert items[0] == int(expected.timestamp())


def test_read_converts_to_column_timezone():
    column = make_column(ZoneInfo("Europe/Berlin"))
    # 2024-07-15 10:00 UTC == 12:00 CEST; offset_naive strips tzinfo after conversion.
    ts = int(datetime(2024, 7, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp())
    assert column.after_read_items([ts]) == (datetime(2024, 7, 15, 12, 0, 0),)


@pytest.mark.parametrize(
    "transition_utc",
    [
        # Europe/Berlin spring forward (01:00 UTC) and fall back (01:00 UTC), 2024
        datetime(2024, 3, 31, 1, 0, 0, tzinfo=timezone.utc),
        datetime(2024, 10, 27, 1, 0, 0, tzinfo=timezone.utc),
    ],
)
def test_read_matches_fromtimestamp_across_dst(transition_utc):
    """The bucketed fast path must agree with fromtimestamp second-by-second
    around a DST transition (including any offset-change-straddling bucket)."""
    tz = ZoneInfo("Europe/Berlin")
    column = make_column(tz)
    base = int(transition_utc.timestamp())
    timestamps = list(range(base - 90, base + 90))
    result = column.after_read_items(timestamps)
    expected = tuple(datetime.fromtimestamp(ts, tz).replace(tzinfo=None) for ts in timestamps)
    assert result == expected


def test_read_nulls_map():
    tz = ZoneInfo("Europe/Berlin")
    column = make_column(tz)
    ts = int(datetime(2024, 7, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp())
    result = column.after_read_items([ts, ts], nulls_map=[False, True])
    assert result == (datetime(2024, 7, 15, 12, 0, 0), None)


def make_column64(tz, scale):
    return DateTime64Column(timezone=tz, offset_naive=True, scale=scale, reader=None, writer=None)


@pytest.mark.parametrize("scale", [0, 3, 6, 9])
def test_datetime64_read_matches_fromtimestamp(scale):
    tz = ZoneInfo("Europe/Berlin")
    column = make_column64(tz, scale)
    base = int(datetime(2024, 7, 15, 10, 0, 0, tzinfo=timezone.utc).timestamp())
    ticks = 10**scale
    items = [base * ticks, base * ticks + (123456789 % ticks if ticks > 1 else 0)]
    result = column.after_read_items(items)
    for got, item in zip(result, items):
        seconds, frac = divmod(item, ticks)
        expected = datetime.fromtimestamp(seconds, tz).replace(tzinfo=None)
        expected = expected.replace(microsecond=frac * 10**6 // ticks if ticks > 1 else 0)
        assert got == expected, (scale, item)


def test_datetime64_read_pre_epoch():
    """DateTime64 supports pre-1970 values (negative ticks): exact floor math."""
    tz = ZoneInfo("Europe/Berlin")
    column = make_column64(tz, 3)
    # 1955-05-15 10:30:00.250 UTC
    target = datetime(1955, 5, 15, 10, 30, 0, tzinfo=timezone.utc)
    ts = int(target.timestamp())
    items = [ts * 1000 + 250]
    (got,) = column.after_read_items(items)
    expected = datetime.fromtimestamp(ts, tz).replace(tzinfo=None, microsecond=250000)
    assert got == expected


@pytest.mark.asyncio
async def test_datetime64_server_roundtrip(conn):
    async with conn.cursor() as cursor:
        await cursor.execute(
            "SELECT toDateTime64('2024-07-15 12:34:56.789123', 6),"
            " toDateTime64('1955-05-15 10:30:00.250', 3)"
        )
        row = await cursor.fetchone()
    assert row == (
        datetime(2024, 7, 15, 12, 34, 56, 789123),
        datetime(1955, 5, 15, 10, 30, 0, 250000),
    )


def test_datetime64_read_across_dst():
    tz = ZoneInfo("Europe/Berlin")
    column = make_column64(tz, 3)
    base = int(datetime(2024, 3, 31, 1, 0, 0, tzinfo=timezone.utc).timestamp())
    items = [(base + delta) * 1000 + 500 for delta in range(-90, 90)]
    result = column.after_read_items(items)
    expected = tuple(
        datetime.fromtimestamp(item // 1000, tz).replace(tzinfo=None, microsecond=500000)
        for item in items
    )
    assert result == expected

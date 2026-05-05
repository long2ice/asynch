"""
PEP 249 — Type objects and constructor compliance tests.

Covers:
- Date, Time, Timestamp constructors return correct Python types
- DateFromTicks, TimeFromTicks, TimestampFromTicks work from Unix timestamps
- Binary wraps bytes/strings into a binary-safe object
- STRING, BINARY, NUMBER, DATETIME, ROWID singletons support == comparison
- Type singletons map correctly to ClickHouse type name strings
- Type singletons are exported from asynch module
"""

import datetime

import pytest

import asynch


class TestDateConstructor:
    def test_returns_date(self):
        result = asynch.Date(2024, 1, 15)
        assert isinstance(result, datetime.date)

    def test_correct_value(self):
        result = asynch.Date(2024, 6, 30)
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 30


class TestTimeConstructor:
    def test_returns_time(self):
        result = asynch.Time(14, 30, 45)
        assert isinstance(result, datetime.time)

    def test_correct_value(self):
        result = asynch.Time(14, 30, 45)
        assert result.hour == 14
        assert result.minute == 30
        assert result.second == 45


class TestTimestampConstructor:
    def test_returns_datetime(self):
        result = asynch.Timestamp(2024, 1, 15, 14, 30, 45)
        assert isinstance(result, datetime.datetime)

    def test_correct_value(self):
        result = asynch.Timestamp(2024, 6, 30, 12, 0, 0)
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 30
        assert result.hour == 12
        assert result.minute == 0
        assert result.second == 0


class TestTicksConstructors:
    # Use a fixed Unix timestamp for deterministic tests
    TICKS = 1_704_067_200  # 2024-01-01 00:00:00 UTC

    def test_date_from_ticks_returns_date(self):
        result = asynch.DateFromTicks(self.TICKS)
        assert isinstance(result, datetime.date)

    def test_time_from_ticks_returns_time(self):
        result = asynch.TimeFromTicks(self.TICKS)
        assert isinstance(result, datetime.time)

    def test_timestamp_from_ticks_returns_datetime(self):
        result = asynch.TimestampFromTicks(self.TICKS)
        assert isinstance(result, datetime.datetime)

    def test_ticks_consistency(self):
        """DateFromTicks and TimestampFromTicks should agree on the date part."""
        d = asynch.DateFromTicks(self.TICKS)
        ts = asynch.TimestampFromTicks(self.TICKS)
        assert d == ts.date()


class TestBinaryConstructor:
    def test_bytes_input(self):
        result = asynch.Binary(b"hello")
        assert isinstance(result, (bytes, bytearray, memoryview))

    def test_string_input(self):
        result = asynch.Binary("hello")
        # Must return a bytes-like object
        assert isinstance(result, (bytes, bytearray, memoryview))

    def test_empty_binary(self):
        result = asynch.Binary(b"")
        assert len(result) == 0


class TestTypeObjectEquality:
    """Type singletons must support == comparison."""

    def test_string_equals_itself(self):
        assert asynch.STRING == asynch.STRING

    def test_number_equals_itself(self):
        assert asynch.NUMBER == asynch.NUMBER

    def test_datetime_equals_itself(self):
        assert asynch.DATETIME == asynch.DATETIME

    def test_binary_equals_itself(self):
        assert asynch.BINARY == asynch.BINARY

    def test_rowid_equals_itself(self):
        assert asynch.ROWID == asynch.ROWID


class TestTypeObjectClickHouseMapping:
    """Type singletons should compare equal to the ClickHouse type name strings they cover."""

    # STRING
    @pytest.mark.parametrize("ch_type", ["String", "FixedString", "UUID", "IPv4", "IPv6"])
    def test_string_covers_type(self, ch_type):
        assert asynch.STRING == ch_type, (
            f"asynch.STRING should equal '{ch_type}' (ClickHouse string-like type)"
        )

    # NUMBER
    @pytest.mark.parametrize(
        "ch_type",
        [
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Float32",
            "Float64",
            "Decimal",
        ],
    )
    def test_number_covers_type(self, ch_type):
        assert asynch.NUMBER == ch_type, (
            f"asynch.NUMBER should equal '{ch_type}' (ClickHouse numeric type)"
        )

    # DATETIME
    @pytest.mark.parametrize("ch_type", ["Date", "Date32", "DateTime", "DateTime64"])
    def test_datetime_covers_type(self, ch_type):
        assert asynch.DATETIME == ch_type, (
            f"asynch.DATETIME should equal '{ch_type}' (ClickHouse date/time type)"
        )

    def test_string_does_not_equal_number_types(self):
        assert not (asynch.STRING == "Int32"), (
            "asynch.STRING should not equal a numeric ClickHouse type"
        )

    def test_number_does_not_equal_string_types(self):
        assert not (asynch.NUMBER == "String"), (
            "asynch.NUMBER should not equal a string ClickHouse type"
        )

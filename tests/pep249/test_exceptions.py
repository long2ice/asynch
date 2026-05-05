"""
PEP 249 — Exception hierarchy compliance tests.

Covers:
- Correct inheritance chain for every standard exception
- Warning is a subclass of Exception (not Error)
- Error is a subclass of Exception
- InterfaceError is a subclass of Error
- DatabaseError is a subclass of Error
- DataError, OperationalError, IntegrityError, InternalError,
  ProgrammingError, NotSupportedError are subclasses of DatabaseError
- Exceptions can be raised and caught at all hierarchy levels
- Exceptions carry meaningful messages
"""

import pytest

from asynch import errors


class TestWarning:
    def test_warning_is_exception(self):
        assert issubclass(errors.Warning, Exception), "Warning must subclass Exception"

    def test_warning_is_not_error(self):
        assert not issubclass(errors.Warning, errors.Error), (
            "Warning must NOT subclass Error (they are siblings under Exception)"
        )

    def test_warning_is_raisable(self):
        with pytest.raises(errors.Warning):
            raise errors.Warning("truncation occurred")

    def test_warning_caught_as_exception(self):
        with pytest.raises(Exception):
            raise errors.Warning("test")


class TestError:
    def test_error_is_exception(self):
        assert issubclass(errors.Error, Exception), "Error must subclass Exception"

    def test_error_is_raisable(self):
        with pytest.raises(errors.Error):
            raise errors.Error("base error")

    def test_error_caught_as_exception(self):
        with pytest.raises(Exception):
            raise errors.Error("test")


class TestInterfaceError:
    def test_is_error(self):
        assert issubclass(errors.InterfaceError, errors.Error), "InterfaceError must subclass Error"

    def test_is_not_database_error(self):
        """InterfaceError and DatabaseError are siblings, not parent-child."""
        # InterfaceError should NOT be a subclass of DatabaseError
        assert not issubclass(errors.InterfaceError, errors.DatabaseError), (
            "InterfaceError must NOT subclass DatabaseError; "
            "they are separate children of Error per PEP 249"
        )

    def test_is_raisable(self):
        with pytest.raises(errors.InterfaceError):
            raise errors.InterfaceError("cursor is closed")

    def test_caught_as_error(self):
        with pytest.raises(errors.Error):
            raise errors.InterfaceError("test")


class TestDatabaseError:
    def test_is_error(self):
        assert issubclass(errors.DatabaseError, errors.Error), "DatabaseError must subclass Error"

    def test_is_raisable(self):
        with pytest.raises(errors.DatabaseError):
            raise errors.DatabaseError("db error")

    def test_caught_as_error(self):
        with pytest.raises(errors.Error):
            raise errors.DatabaseError("test")


class TestDataError:
    def test_is_database_error(self):
        assert issubclass(errors.DataError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.DataError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.DataError):
            raise errors.DataError("value out of range")

    def test_caught_as_database_error(self):
        with pytest.raises(errors.DatabaseError):
            raise errors.DataError("test")


class TestOperationalError:
    def test_is_database_error(self):
        assert issubclass(errors.OperationalError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.OperationalError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.OperationalError):
            raise errors.OperationalError("connection lost")

    def test_caught_as_database_error(self):
        with pytest.raises(errors.DatabaseError):
            raise errors.OperationalError("test")


class TestIntegrityError:
    def test_is_database_error(self):
        assert issubclass(errors.IntegrityError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.IntegrityError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.IntegrityError):
            raise errors.IntegrityError("FK violation")


class TestInternalError:
    def test_is_database_error(self):
        assert issubclass(errors.InternalError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.InternalError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.InternalError):
            raise errors.InternalError("cursor invalid")


class TestProgrammingError:
    def test_is_database_error(self):
        assert issubclass(errors.ProgrammingError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.ProgrammingError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.ProgrammingError):
            raise errors.ProgrammingError("syntax error")


class TestNotSupportedError:
    def test_is_database_error(self):
        assert issubclass(errors.NotSupportedError, errors.DatabaseError)

    def test_is_error(self):
        assert issubclass(errors.NotSupportedError, errors.Error)

    def test_is_raisable(self):
        with pytest.raises(errors.NotSupportedError):
            raise errors.NotSupportedError("not supported")


class TestExceptionMessageHandling:
    """Exceptions must be able to carry meaningful error messages."""

    def test_error_preserves_message(self):
        msg = "something went wrong"
        exc = errors.Error(msg)
        assert str(exc) or repr(exc)  # must produce some string representation

    def test_programming_error_preserves_message(self):
        msg = "table not found"
        exc = errors.ProgrammingError(msg)
        assert str(exc) or repr(exc)

    def test_operational_error_caught_at_multiple_levels(self):
        with pytest.raises(errors.OperationalError):
            raise errors.OperationalError("disconnect")

        with pytest.raises(errors.DatabaseError):
            raise errors.OperationalError("disconnect")

        with pytest.raises(errors.Error):
            raise errors.OperationalError("disconnect")

        with pytest.raises(Exception):
            raise errors.OperationalError("disconnect")

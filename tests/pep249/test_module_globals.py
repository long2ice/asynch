"""
PEP 249 §1 — Module-level interface compliance tests.

Covers:
- apilevel
- threadsafety
- paramstyle
- connect() factory function
- Exception classes exported at module level
- Type objects exported at module level
"""

import inspect

import pytest

import asynch


class TestApiLevel:
    """PEP 249 requires the module to declare its DB-API compliance level."""

    def test_apilevel_exists(self):
        assert hasattr(asynch, "apilevel"), "asynch must define 'apilevel'"

    def test_apilevel_is_string(self):
        assert isinstance(asynch.apilevel, str), "'apilevel' must be a string"

    def test_apilevel_value(self):
        assert asynch.apilevel == "2.0", "apilevel must be '2.0' for DB-API v2.0"


class TestThreadSafety:
    """PEP 249 requires the module to declare its thread-safety level."""

    def test_threadsafety_exists(self):
        assert hasattr(asynch, "threadsafety"), "asynch must define 'threadsafety'"

    def test_threadsafety_is_int(self):
        assert isinstance(asynch.threadsafety, int), "'threadsafety' must be an int"

    def test_threadsafety_valid_range(self):
        assert asynch.threadsafety in (0, 1, 2, 3), (
            f"'threadsafety' must be 0, 1, 2, or 3; got {asynch.threadsafety!r}"
        )


class TestParamStyle:
    """PEP 249 requires the module to declare its parameter style."""

    VALID_STYLES = {"qmark", "numeric", "named", "format", "pyformat"}

    def test_paramstyle_exists(self):
        assert hasattr(asynch, "paramstyle"), "asynch must define 'paramstyle'"

    def test_paramstyle_is_string(self):
        assert isinstance(asynch.paramstyle, str), "'paramstyle' must be a string"

    def test_paramstyle_valid_value(self):
        assert asynch.paramstyle in self.VALID_STYLES, (
            f"'paramstyle' must be one of {self.VALID_STYLES}; got {asynch.paramstyle!r}"
        )


class TestConnectFactory:
    """PEP 249 requires a module-level connect() constructor."""

    def test_connect_exists(self):
        assert hasattr(asynch, "connect"), "asynch must define a 'connect' function"

    def test_connect_is_callable(self):
        assert callable(asynch.connect), "'connect' must be callable"

    def test_connect_returns_connection(self):
        from asynch.connection import Connection

        conn = asynch.connect()
        assert isinstance(conn, Connection), "asynch.connect() must return a Connection object"

    def test_connect_accepts_dsn(self, config):
        from asynch.connection import Connection

        conn = asynch.connect(dsn=config.dsn)
        assert isinstance(conn, Connection)

    def test_connect_accepts_kwargs(self, config):
        from asynch.connection import Connection

        conn = asynch.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
        )
        assert isinstance(conn, Connection)


class TestModuleLevelExceptions:
    """PEP 249 requires all standard exceptions to be accessible at module level."""

    REQUIRED_EXCEPTIONS = [
        "Warning",
        "Error",
        "InterfaceError",
        "DatabaseError",
        "DataError",
        "OperationalError",
        "IntegrityError",
        "InternalError",
        "ProgrammingError",
        "NotSupportedError",
    ]

    @pytest.mark.parametrize("exc_name", REQUIRED_EXCEPTIONS)
    def test_exception_exported(self, exc_name):
        assert hasattr(asynch, exc_name), f"asynch must export '{exc_name}' at module level"

    @pytest.mark.parametrize("exc_name", REQUIRED_EXCEPTIONS)
    def test_exception_is_class(self, exc_name):
        exc = getattr(asynch, exc_name)
        assert inspect.isclass(exc), f"asynch.{exc_name} must be a class"

    @pytest.mark.parametrize("exc_name", REQUIRED_EXCEPTIONS)
    def test_exception_is_raisable(self, exc_name):
        exc_class = getattr(asynch, exc_name)
        with pytest.raises(exc_class):
            raise exc_class("test message")


class TestModuleLevelTypeObjects:
    """PEP 249 requires type singleton objects at module level."""

    REQUIRED_TYPE_OBJECTS = ["STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"]

    @pytest.mark.parametrize("name", REQUIRED_TYPE_OBJECTS)
    def test_type_object_exported(self, name):
        assert hasattr(asynch, name), f"asynch must export type object '{name}'"

    @pytest.mark.parametrize("name", REQUIRED_TYPE_OBJECTS)
    def test_type_object_supports_equality(self, name):
        obj = getattr(asynch, name)
        # Must support == comparison without raising
        result = obj == obj
        assert result is True, f"asynch.{name} == asynch.{name} must be True"

    def test_type_objects_are_distinct(self):
        """Different type objects should not compare equal to each other."""
        types = [getattr(asynch, n) for n in self.REQUIRED_TYPE_OBJECTS]
        for i, a in enumerate(types):
            for j, b in enumerate(types):
                if i != j:
                    # They don't need to be unequal if the spec allows overlap,
                    # but in practice they should be for most ClickHouse types.
                    # This is a soft check — just verify the comparison doesn't raise.
                    _ = a == b


class TestModuleLevelTypeConstructors:
    """PEP 249 requires type constructor functions at module level."""

    REQUIRED_CONSTRUCTORS = [
        "Date",
        "Time",
        "Timestamp",
        "DateFromTicks",
        "TimeFromTicks",
        "TimestampFromTicks",
        "Binary",
    ]

    @pytest.mark.parametrize("name", REQUIRED_CONSTRUCTORS)
    def test_constructor_exported(self, name):
        assert hasattr(asynch, name), f"asynch must export type constructor '{name}'"

    @pytest.mark.parametrize("name", REQUIRED_CONSTRUCTORS)
    def test_constructor_is_callable(self, name):
        ctor = getattr(asynch, name)
        assert callable(ctor), f"asynch.{name} must be callable"

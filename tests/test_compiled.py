"""Guard that the suite actually exercises the compiled extensions.

Without this, a build regression (or stale .py leftovers shadowing the .so)
would silently turn the whole test run into a pure-Python one.
"""

import importlib

import pytest

COMPILED_MODULES = [
    "asynch.proto.block",
    "asynch.proto.streams.buffered",
    "asynch.proto.streams.block",
    "asynch.proto.streams.compressed",
    "asynch.proto.columns",
    "asynch.proto.columns.base",
    "asynch.proto.columns.stringcolumn",
    "asynch.proto.columns.intcolumn",
    "asynch.proto.columns.arraycolumn",
    "asynch.proto.columns.datetimecolumn",
    "asynch.proto.columns.lowcardinalitycolumn",
    "asynch.proto.columns.mapcolumn",
    "asynch.proto.columns.nullablecolumn",
    "asynch.proto.columns.tuplecolumn",
    "asynch.proto.columns.uuidcolumn",
    "asynch.proto.connection",
    "asynch.proto.result",
    "asynch.proto.progress",
    "asynch.cursors",
]


@pytest.mark.parametrize("module_name", COMPILED_MODULES)
def test_module_is_compiled(module_name):
    module = importlib.import_module(module_name)
    assert module.__file__ is not None
    assert module.__file__.endswith((".so", ".pyd")), (
        f"{module_name} resolved to {module.__file__}; expected a compiled extension"
    )

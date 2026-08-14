import os
import subprocess
import sys
import sysconfig
import textwrap
from concurrent.futures import ThreadPoolExecutor

import pytest

from asynch.proto.streams.buffered import encode_varint
from asynch.proto.utils.escape import escape_param

MODULES = [
    "asynch.connection",
    "asynch.cursors",
    "asynch.errors",
    "asynch.pool",
    "asynch.proto.block",
    "asynch.proto.columns",
    "asynch.proto.connection",
    "asynch.proto.result",
    "asynch.proto.streams.buffered",
    "asynch.proto.streams.block",
    "asynch.proto.streams.compressed",
]

free_threaded_only = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"),
    reason="requires a free-threaded CPython build",
)


@free_threaded_only
def test_imports_do_not_enable_gil():
    """A module that has not declared freethreading_compatible re-enables the
    GIL for the whole process on import, which defeats the point of running a
    free-threaded build at all."""
    code = textwrap.dedent(
        """
        import sys
        {imports}
        assert not sys._is_gil_enabled(), "importing asynch re-enabled the GIL"
        """
    ).format(imports="\n".join(f"import {name}" for name in MODULES))
    env = os.environ.copy()
    # PYTHON_GIL=0 would force the GIL off and hide exactly what we are testing.
    env.pop("PYTHON_GIL", None)
    env["PYTHONNOUSERSITE"] = "1"
    subprocess.run([sys.executable, "-c", code], check=True, env=env)


@free_threaded_only
def test_shared_tables_run_from_multiple_threads():
    """The declaration only claims module-level state is safe. This exercises
    that claim on the shared tables: the escape table, the column dispatcher
    and varint encoding, hit concurrently."""
    from asynch.proto.columns import column_by_type

    def worker(iterations: int) -> int:
        total = 0
        for _ in range(iterations):
            total += len(escape_param("a 'quoted' \\ text"))
            total += len(escape_param([1, "x", None]))
            total += len(encode_varint(2**63 - 1))
            total += len(column_by_type)
        return total

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(worker, [1000] * 4))

    # Same deterministic input in every thread: divergence would mean the
    # shared tables were being mutated underneath.
    assert results == [results[0]] * 4

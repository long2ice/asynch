"""Regenerate the .pyi stubs for the Cython modules.

stubgen-pyx parses the .pyx sources, so unlike mypy's stubgen it recovers real
signatures for cdef classes instead of (*args, **kwargs). It gets a few things
wrong that this script patches afterwards, so regeneration stays a single
reproducible command:

    make stubs

`make check` runs stubtest afterwards; it compares every stub against the
compiled module and fails on drift.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parent.parent / "asynch"

# The .pyx sources carry no return annotations, so every generated signature
# would end in an implicit Any and callers would get no attribute checking on
# what they get back. Annotating the sources instead would hand the types to
# Cython's annotation_typing and change codegen, so the public surface is
# annotated here. {relative stub path: {function: return type}}
RETURN_TYPES: dict[str, dict[str, str]] = {
    "cursors.pyi": {
        "execute": "int",
        "executemany": "int",
        "fetchone": "Any",
        "fetchall": "list[Any]",
        "fetchmany": "list[Any]",
        "close": "None",
        "__aenter__": "Self",
    },
    "proto/connection.pyi": {
        "connect": "None",
        "disconnect": "None",
        "ping": "bool",
        "execute": "Any",
        "execute_iter": "Any",
        "execute_with_progress": "Any",
        "substitute_params": "str",
    },
}

# Imports the annotations above need, appended after the generated imports.
EXTRA_IMPORTS: dict[str, str] = {
    "cursors.pyi": ("from typing import Any\n\nfrom typing_extensions import Self\n"),
    "proto/connection.pyi": "from typing import Any\n",
}

# (relative stub path, before, after, reason)
PATCHES: list[tuple[str, str, str, str]] = [
    (
        "proto/streams/buffered.pyi",
        "class BufferedWriter:\n    def __init__(",
        (
            "class BufferedWriter:\n"
            "    writer: Any\n"
            "    buffer: bytearray\n"
            "    position: int\n"
            "    max_buffer_size: int\n"
            "    def __init__("
        ),
        (
            "stubgen-pyx does not emit instance attributes assigned in plain-class "
            "__init__; downstream modules (e.g. proto/compression) touch them"
        ),
    ),
    (
        "proto/streams/buffered.pyi",
        "class BufferedReader:\n    def __init__(",
        (
            "class BufferedReader:\n"
            "    reader: Any\n"
            "    buffer: bytearray\n"
            "    position: int\n"
            "    current_buffer_size: int\n"
            "    buffer_max_size: int\n"
            "    def __init__("
        ),
        "same as BufferedWriter: downstream code reads/assigns these attributes",
    ),
    (
        "proto/streams/buffered.pyi",
        "from asynch.proto import constants",
        "from typing import Any\n\nfrom asynch.proto import constants",
        "the injected attribute declarations above need Any",
    ),
    (
        "proto/block.pyi",
        (
            "    dict_row_types = dict\n"
            "    tuple_row_types = (list, tuple)\n"
            "    supported_row_types = ..."
        ),
        (
            "    dict_row_types: tuple[type, ...]\n"
            "    tuple_row_types: tuple[type, ...]\n"
            "    supported_row_types: tuple[type, ...]"
        ),
        (
            "stubgen-pyx mis-renders the class-level type tuples; stubtest then "
            "compares runtime tuples of constructors against a bare `dict`"
        ),
    ),
    (
        "proto/columns/__init__.pyi",
        "column_by_type = ...",
        "column_by_type: dict",
        "the initializer builds a dict of column classes, which stubgen-pyx cannot evaluate",
    ),
    (
        "proto/columns/lowcardinalitycolumn.pyi",
        "serialization_type = ...",
        "serialization_type: int",
        "computed from int flags at class-body time; stubgen-pyx cannot evaluate the expression",
    ),
    (
        "proto/columns/datecolumn.pyi",
        ("lazy_date_lut = LazyLUT(_factory=...)\nlazy_date_lut_reverse = LazyLUT(_factory=...)"),
        ("lazy_date_lut: LazyLUT\nlazy_date_lut_reverse: LazyLUT"),
        (
            "the assignments reference LazyLUT before its class statement; "
            "annotation form keeps the forward reference legal in a stub"
        ),
    ),
]


def apply_return_types(path: Path, returns: dict[str, str]) -> list[str]:
    """Give each named def an explicit return type, leaving parameters alone."""
    source = path.read_text()
    missing = []
    for name, return_type in returns.items():
        # `    async def fetchone(self):` -> `    async def fetchone(self) -> Any:`
        # Matches whatever parameter list is currently generated, so a signature
        # change does not silently skip the annotation.
        pattern = re.compile(
            r"^(?P<indent>[ ]*)(?P<async>async )?def (?P<name>%s)"
            r"\((?P<params>.*)\)(?P<ret>[ ]*->[^:]+)?:" % re.escape(name),
            re.MULTILINE,
        )
        source, count = pattern.subn(
            lambda m: (
                "%s%sdef %s(%s) -> %s:"
                % (
                    m.group("indent"),
                    m.group("async") or "",
                    m.group("name"),
                    m.group("params"),
                    return_type,
                )
            ),
            source,
        )
        if not count:
            missing.append(name)
    path.write_text(source)
    return missing


def main() -> int:
    subprocess.run(["stubgen-pyx", str(PACKAGE)], check=True)  # noqa: S603,S607

    # The generated header embeds the absolute .pyx path: machine-specific and
    # over the line-length limit. Rewrite it to a repo-relative path.
    # Also normalize class-level `py_types = (...)` tuples: stubgen-pyx renders
    # them as literal constructor references, which stubtest then compares
    # against runtime tuples of types and rejects.
    repo_root = PACKAGE.parent
    for stub in PACKAGE.glob("**/*.pyi"):
        source = stub.read_text()
        lines = source.splitlines(keepends=True)
        if lines and "generated by stubgen-pyx" in lines[0]:
            # Shortened (no version, relative path) so the longest module
            # path still fits the line-length limit.
            lines[0] = (
                f"# Generated by stubgen-pyx from "
                f"{stub.relative_to(repo_root).with_suffix('.pyx')}\n"
            )
            source = "".join(lines)
        # `| None` because the Column base class leaves it None until a
        # concrete subclass fills it in.
        source = re.sub(
            r"^(\s*)py_types = .*$",
            r"\1py_types: tuple[type, ...] | None",
            source,
            flags=re.MULTILINE,
        )
        stub.write_text(source)

    for filename, before, after, reason in PATCHES:
        path = PACKAGE / filename
        source = path.read_text()
        if before not in source:
            print(  # noqa: T201
                f"warning: {filename} no longer contains the text patched for "
                f"'{reason}'. stubgen-pyx may have fixed it upstream — drop the "
                f"entry from {Path(__file__).name} if so.",
                file=sys.stderr,
            )
            continue
        path.write_text(source.replace(before, after, 1))
        print(f"patched {filename}: {reason}")  # noqa: T201

    failed = False
    for filename, returns in RETURN_TYPES.items():
        path = PACKAGE / filename
        source = path.read_text()
        extra = EXTRA_IMPORTS.get(filename, "")
        if extra and extra not in source:
            # After the generated header comment, before the first declaration.
            lines = source.splitlines(keepends=True)
            insert_at = 1 if lines and lines[0].startswith("#") else 0
            source = "".join(lines[:insert_at]) + extra + "".join(lines[insert_at:])
            path.write_text(source)
        missing = apply_return_types(path, returns)
        print(f"annotated {filename}: {len(returns) - len(missing)} return types")  # noqa: T201
        if missing:
            failed = True
            print(  # noqa: T201
                f"error: {filename} has no def for {sorted(missing)} — the API "
                f"changed, so update RETURN_TYPES in {Path(__file__).name}",
                file=sys.stderr,
            )

    # Format here so the generated files satisfy `make check` as-is, rather
    # than every regeneration leaving the tree dirty. The package is nested,
    # so glob recursively (unlike asyncmy's flat layout).
    stubs = sorted(str(p) for p in PACKAGE.glob("**/*.pyi"))
    if stubs:
        # Fix first, then format: autofixes (e.g. unused-import removal) can
        # leave blank-line artifacts that only the formatter cleans up.
        subprocess.run(["ruff", "check", "--quiet", "--fix", *stubs], check=False)  # noqa: S603,S607
        subprocess.run(["ruff", "format", "--quiet", *stubs], check=True)  # noqa: S603,S607
    print(f"formatted {len(stubs)} stub files")  # noqa: T201

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

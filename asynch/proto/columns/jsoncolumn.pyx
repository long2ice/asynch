# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
"""ClickHouse `JSON` column (and the legacy `Object('json')` spelling).

The wire format is the one introduced in 24.8 and stabilised in 25.x. Both
directions are composed out of existing column readers rather than
hand-rolled: `DynamicColumn` reads one dynamic path's body, and the shared
sub-column is an `Array(Tuple(String, String))`.
"""

import json

from .base import Column
from .dynamiccolumn import (
    DYNAMIC_V2,
    SHARED_VARIANT_NAME,
    VARIANT_MODE_BASIC,
    DynamicColumn,
    SharedValueDecoder,
)
from .stringcolumn import String

# ObjectSerializationVersion
OBJECT_V1 = 0
OBJECT_STRING = 1
OBJECT_V2 = 2


class JsonColumn(Column):
    """Reads a JSON column into nested dicts; writes dicts or JSON text.

    Reading covers V1/V2 framing and the string fallback. Writing emits V2,
    which the server downgrades by itself when the client revision is older.
    """

    py_types = (dict, str)

    # JSON columns are not nullable on the wire; this is the placeholder the
    # base column's null handling uses.
    null_value = {}

    # The prefix (path list plus per-path Dynamic prefixes) depends on the
    # data, so the block's items have to reach `write_state_prefix`.
    prefix_needs_items = True

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self._column_kwargs = kwargs
        self.string_column = String(**kwargs)

        self.serialization_version = None
        self._write_paths_state = None
        self.sorted_dynamic_paths = []
        self.dynamic_columns = []
        self.shared_data_column = None
        # One decoder per column: its type and column caches then accumulate
        # over every overflow value in the block, including those reached
        # through nested Dynamic columns.
        self._shared_value_decoder = SharedValueDecoder(self._column_with_reader)
        super().__init__(**kwargs)

    def _column_with_reader(self, spec, reader=None):
        """Build a column for `spec`, optionally bound to another reader.

        Imported late: this module is itself imported by the dispatcher.
        """
        from asynch.proto.columns import get_column_by_spec

        options = dict(self._column_kwargs)
        if reader is not None:
            options["reader"] = reader
        return get_column_by_spec(spec, options)

    # -- read ---------------------------------------------------------------

    async def read_state_prefix(self):
        # ObjectStructure stream:
        #   UInt64  serialization_version
        #   if V1:  VarUInt legacy max_dynamic_paths, discarded
        #   VarUInt dynamic_paths_count
        #   N x String path names, sorted
        # then one SerializationDynamic prefix per path, then the shared-data
        # prefix (Array/Tuple/String emit none).
        version = await self.reader.read_uint64()
        if version not in (OBJECT_V1, OBJECT_STRING, OBJECT_V2):
            raise NotImplementedError(f"Unsupported JSON serialization version {version}")
        self.serialization_version = version
        if version == OBJECT_STRING:
            return

        if version == OBJECT_V1:
            await self.reader.read_varint()

        num_paths = await self.reader.read_varint()
        self.sorted_dynamic_paths = [await self.reader.read_str() for _ in range(num_paths)]

        self.dynamic_columns = []
        for _ in range(num_paths):
            column = DynamicColumn(
                self._column_with_reader,
                shared_value_decoder=self._shared_value_decoder,
                **self._column_kwargs,
            )
            await column.read_state_prefix()
            self.dynamic_columns.append(column)

        self.shared_data_column = self._column_with_reader("Array(Tuple(String, String))")
        await self.shared_data_column.read_state_prefix()

    async def read_items(self, n_items):
        if self.serialization_version == OBJECT_STRING:
            strings = await self.reader.read_strs(n_items)
            return [json.loads(s) if s else {} for s in strings]

        per_path_values = []
        for column in self.dynamic_columns:
            per_path_values.append(await column.read_items(n_items))

        shared_rows = await self.shared_data_column.read_data(n_items)
        return await self._fold_rows(n_items, per_path_values, shared_rows)

    async def _fold_rows(self, n_items, per_path_values, shared_rows):
        rows = [{} for _ in range(n_items)]

        for path, values in zip(self.sorted_dynamic_paths, per_path_values):
            for row_idx, value in enumerate(values):
                if value is not None:
                    rows[row_idx][path] = _tuples_to_lists(value)

        decode = self._shared_value_decoder.decode
        for row_idx, entries in enumerate(shared_rows or []):
            for path, encoded in entries:
                # The shared sub-column is declared String, but its values are
                # raw encodeDataType blobs rather than text.
                if isinstance(encoded, str):
                    encoded = encoded.encode("utf-8", "surrogateescape")
                rows[row_idx][path] = _tuples_to_lists(await decode(encoded))

        for row in rows:
            _denormalize_dotted_paths(row)
        return rows

    # -- write --------------------------------------------------------------

    async def write_state_prefix(self, items=None):
        if items is None:
            raise NotImplementedError(
                "JSON columns can only be written when the block items reach "
                "write_state_prefix"
            )
        normalised = [self._normalise_write_item(x) for x in items]
        self._write_paths_state = _unfold_json(normalised, depth=0)
        await self._write_object_state_prefix(self._write_paths_state)

    async def write_items(self, items):
        # The body comes from the paths computed over the same items in
        # write_state_prefix; `items` only supplies the row count here.
        await self._write_values(self._write_paths_state, len(items))

    def _normalise_write_item(self, item):
        if item is None:
            return self.null_value
        if isinstance(item, str):
            return json.loads(item)
        return item

    async def _write_object_state_prefix(self, paths):
        # V2 unconditionally: the server falls back to V1 framing itself when
        # the client revision is too old for V2.
        await self.writer.write_uint64(OBJECT_V2)
        await self.writer.write_varint(len(paths))
        await self.string_column.write_items(list(paths.keys()))
        for variants in paths.values():
            # SerializationDynamic prefix per path.
            await self.writer.write_uint64(DYNAMIC_V2)
            await self.writer.write_varint(len(variants))
            await self.string_column.write_items(list(variants.keys()))
            await self.writer.write_uint64(VARIANT_MODE_BASIC)

    async def _write_values(self, paths, rows):
        for variants in paths.values():
            await self.writer.write_bytes(_row_discriminators(variants, rows))
            for spec, data in variants.items():
                column = self._column_with_reader(spec)
                if spec.startswith("Array"):
                    await column.write_data(data["values"])
                else:
                    await column.write_items(data["values"])
        # Shared-data offsets: all zero, since the write path never overflows
        # into the SharedVariant.
        await self.writer.write_bytes(b"\x00" * rows * 8)


def _tuples_to_lists(value):
    """Tuples read back from Array/Tuple columns become lists, so a JSON
    round-trip compares equal to the dict that was written."""
    if isinstance(value, tuple):
        return [_tuples_to_lists(x) for x in value]
    if isinstance(value, list):
        return [_tuples_to_lists(x) for x in value]
    return value


def _denormalize_dotted_paths(obj):
    """Turn dotted top-level keys ("a.b.c") back into nested dicts."""
    for key in list(obj.keys()):
        parts = key.split(".")
        if len(parts) <= 1:
            continue
        parent = obj
        for part in parts[:-1]:
            child = parent.get(part)
            if child is None:
                child = {}
                parent[part] = child
            elif not isinstance(child, dict):
                raise ValueError(
                    f"Cannot denormalise dotted JSON path {key!r}: prefix "
                    f"{part!r} already holds a non-dict value {child!r}"
                )
            parent = child
        parent[parts[-1]] = obj[key]
        del obj[key]


def _value_spec(item):
    """The ClickHouse spec to write a Python value into a JSON path as."""
    if isinstance(item, bool):
        return "Bool"
    if isinstance(item, int):
        return "Int64"
    if isinstance(item, float):
        return "Float64"
    if isinstance(item, str):
        return "String"
    if isinstance(item, list):
        types = {type(x) for x in item if x is not None}
        if not types or types <= {str}:
            return "Array(Nullable(String))"
        if types <= {bool}:
            return "Array(Nullable(Bool))"
        if types <= {int, bool}:
            return "Array(Nullable(Int64))"
        if types <= {int, float, bool}:
            return "Array(Nullable(Float64))"
        return "Array(Nullable(String))"
    if item is None:
        return "String"
    return "String"


def _flatten(obj):
    """Flatten nested dicts into dotted-path leaves: {"a": {"b": 1}} ->
    {"a.b": 1}. A non-dict leaf comes back as {"": leaf} so callers can
    compose the key."""
    if isinstance(obj, dict):
        result = {}
        for key, value in obj.items():
            if value is None:
                continue
            for suffix, leaf in _flatten(value).items():
                result[f"{key}.{suffix}"] = leaf
        return result
    return {"": obj}


def _unfold_json(items, depth):
    """Build {path: {spec: {values, positions}}} for a whole INSERT block,
    sorted so the on-wire order matches what the server expects."""
    result = {}
    for row, obj in enumerate(items):
        for key, value in obj.items():
            if value is None:
                continue
            for suffix, leaf in _flatten(value).items():
                path = f"{key}.{suffix}"
                per_spec = result.setdefault(path, {})
                spec = _value_spec(leaf)
                entry = per_spec.setdefault(spec, {"values": [], "positions": []})
                entry["values"].append(leaf)
                entry["positions"].append(row)

    # Keys were built with a trailing "." from the {"": leaf} convention.
    folded = {}
    for path, per_spec in result.items():
        folded[path[:-1]] = dict(sorted(per_spec.items()))
    return dict(sorted(folded.items()))


def _row_discriminators(variants, row_count):
    """Global discriminator per row: the position of each variant in the
    alphabetical sort of the variant names, which includes SharedVariant."""
    sorted_names = sorted([*variants.keys(), SHARED_VARIANT_NAME])
    spec_to_discriminator = {spec: i for i, spec in enumerate(sorted_names)}
    result = [0xFF] * row_count
    for spec, data in variants.items():
        discriminator = spec_to_discriminator[spec]
        for position in data["positions"]:
            result[position] = discriminator
    return bytes(result)


class LegacyJsonColumn(Column):
    """The pre-24.8 `Object('json')` layout.

    Servers from 24.8 onwards removed the type entirely, so this only matters
    when talking to an older one; its framing (UInt8 version, then a type
    spec string) is incompatible with the `JSON` format above.
    """

    py_types = (dict,)
    null_value = {}

    def __init__(self, column_by_spec_getter, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        self.string_column = String(**kwargs)
        super().__init__(**kwargs)

    async def write_state_prefix(self):
        await self.writer.write_uint8(1)

    async def read_items(self, n_items):
        await self.reader.read_uint8()
        spec = await self.reader.read_str()
        column = self.column_by_spec_getter(spec)
        await column.read_state_prefix()
        return await column.read_data(n_items)

    async def write_items(self, items):
        await self.string_column.write_items(
            [x if isinstance(x, str) else json.dumps(x) for x in items]
        )


def create_json_column(spec, column_by_spec_getter, column_options):
    if spec.startswith("Object("):
        return LegacyJsonColumn(column_by_spec_getter, **column_options)
    return JsonColumn(column_by_spec_getter, **column_options)

# cython: freethreading_compatible=True
#
# Module-level state in this module is built at import time and treated as
# read-only afterwards, which is what makes the module safe to import and use
# from multiple threads under free-threaded CPython. This does NOT make a
# single column instance safe to share between threads.
"""Reader for ClickHouse's Dynamic / Variant column families.

These are not user-facing types yet: they exist so the JSON column can be
composed out of them instead of hand-rolling Variant deserialization. The
byte layout mirrors `SerializationDynamic` and `SerializationVariant`.
"""

from .base import Column
from .stringcolumn import ByteString

# DynamicSerializationVersion. These values are independent of the JSON
# column's ObjectSerializationVersion, which numbers its own enum.
DYNAMIC_V1 = 1
DYNAMIC_V2 = 2

# VariantDiscriminatorsSerializationMode
VARIANT_MODE_BASIC = 0
VARIANT_MODE_COMPACT = 1

# ColumnVariant::NULL_DISCRIMINATOR - marks a NULL row.
NULL_DISCRIMINATOR = 0xFF

# SHARED_VARIANT_TYPE_NAME. It takes part in the alphabetical sort of variant
# names alongside the declared ones, so its discriminator has to be derived
# rather than assumed to be last.
SHARED_VARIANT_NAME = "SharedVariant"


class DynamicColumn(Column):
    """A ClickHouse `Dynamic` column.

    The variant types are not known at construction: they arrive on the wire
    in `read_state_prefix`. The list always ends with an implicit
    `SharedVariant` - a byte string carrying `encodeDataType +
    serializeBinary` blobs - which catches values whose type matches none of
    the declared variants.
    """

    py_types = (object,)

    def __init__(self, column_by_spec_getter, shared_value_decoder=None, **kwargs):
        self.column_by_spec_getter = column_by_spec_getter
        # Sharing one decoder across every DynamicColumn of a block (and the
        # JSON column itself) lets its type/column caches accumulate over all
        # overflow values in the block.
        if shared_value_decoder is None:
            shared_value_decoder = SharedValueDecoder(column_by_spec_getter)
        self.shared_value_decoder = shared_value_decoder
        self._column_kwargs = kwargs
        self.variant_specs = []
        self.variant_columns = []
        self.discriminators_mode = VARIANT_MODE_BASIC
        self._shared_variant_index = None
        super().__init__(**kwargs)

    async def read_state_prefix(self):
        # ObjectStructure stream:
        #   UInt64  structure_version (V1 or V2)
        #   if V1:  VarUInt legacy slot, discarded by the server too
        #   VarUInt num_dynamic_types (excludes the implicit SharedVariant)
        #   N x String variant type spec
        # then SerializationVariant prefix:
        #   UInt64  discriminators_mode
        structure_version = await self.reader.read_uint64()
        if structure_version not in (DYNAMIC_V1, DYNAMIC_V2):
            raise NotImplementedError(
                f"Unsupported Dynamic serialization version {structure_version}"
            )
        if structure_version == DYNAMIC_V1:
            await self.reader.read_varint()

        num_dynamic_types = await self.reader.read_varint()
        declared_specs = [await self.reader.read_str() for _ in range(num_dynamic_types)]

        # Global discriminators are assigned by sorting the type names
        # alphabetically, and "SharedVariant" sorts among them, so its index
        # has to be recovered from the same sort rather than assumed.
        sorted_names = sorted([*declared_specs, SHARED_VARIANT_NAME])
        self.variant_specs = list(declared_specs)
        self.variant_columns = []
        self._shared_variant_index = None
        for i, name in enumerate(sorted_names):
            if name == SHARED_VARIANT_NAME:
                self.variant_columns.append(ByteString(**self._column_kwargs))
                self._shared_variant_index = i
            else:
                self.variant_columns.append(self.column_by_spec_getter(name))

        self.discriminators_mode = await self.reader.read_uint64()
        if self.discriminators_mode not in (VARIANT_MODE_BASIC, VARIANT_MODE_COMPACT):
            raise NotImplementedError(
                f"Unsupported Variant discriminators mode {self.discriminators_mode}"
            )

        # Most primitives have no prefix, but a composed variant
        # (LowCardinality and friends) has to consume its own bytes.
        for i, column in enumerate(self.variant_columns):
            if i == self._shared_variant_index:
                continue
            await column.read_state_prefix()

    async def read_items(self, n_items):
        # SerializationVariant body, BASIC mode:
        #   n_items x UInt8 global discriminators (255 = NULL)
        #   then, in discriminator order, each variant's column data sized by
        #   how many rows landed in it.
        if self.discriminators_mode == VARIANT_MODE_COMPACT:
            raise NotImplementedError("Compact Variant discriminators are not supported yet")

        variant_columns = self.variant_columns
        shared_variant_index = self._shared_variant_index
        decode_shared = self.shared_value_decoder.decode

        discriminators = await self.reader.read_bytes(n_items)
        if len(discriminators) != n_items:
            raise EOFError(
                f"Variant discriminators truncated: got {len(discriminators)} bytes, "
                f"want {n_items}"
            )

        per_variant_rows = [[] for _ in range(len(variant_columns))]
        for row, disc in enumerate(discriminators):
            if disc == NULL_DISCRIMINATOR:
                continue
            if disc >= len(variant_columns):
                raise ValueError(
                    f"Variant discriminator {disc} out of range "
                    f"(have {len(variant_columns)} variants)"
                )
            per_variant_rows[disc].append(row)

        values_by_row = [None] * n_items
        for variant_idx, rows in enumerate(per_variant_rows):
            if not rows:
                continue
            column = variant_columns[variant_idx]
            chunk = list(await column.read_data(len(rows)))
            if variant_idx == shared_variant_index:
                chunk = [await decode_shared(blob) for blob in chunk]
            for row, value in zip(rows, chunk):
                values_by_row[row] = value

        return values_by_row


# --------------------------------------------------------------------------
# encodeDataType / serializeBinary decoding for SharedVariant blobs
# --------------------------------------------------------------------------

# BinaryTypeIndex tags. Only the ones reachable from a value that can appear
# in a JSON document are mapped; anything else raises so gaps surface loudly
# instead of silently dropping data.
_TAG_NOTHING = 0x00
_TAG_UINT8 = 0x01
_TAG_UINT16 = 0x02
_TAG_UINT32 = 0x03
_TAG_UINT64 = 0x04
_TAG_UINT128 = 0x05
_TAG_UINT256 = 0x06
_TAG_INT8 = 0x07
_TAG_INT16 = 0x08
_TAG_INT32 = 0x09
_TAG_INT64 = 0x0A
_TAG_INT128 = 0x0B
_TAG_INT256 = 0x0C
_TAG_FLOAT32 = 0x0D
_TAG_FLOAT64 = 0x0E
_TAG_DATE = 0x0F
_TAG_DATE32 = 0x10
_TAG_DATETIME_UTC = 0x11
_TAG_DATETIME_TZ = 0x12
_TAG_DATETIME64_UTC = 0x13
_TAG_DATETIME64_TZ = 0x14
_TAG_STRING = 0x15
_TAG_FIXED_STRING = 0x16
_TAG_ARRAY = 0x1E
_TAG_TUPLE_UNNAMED = 0x1F
_TAG_TUPLE_NAMED = 0x20
_TAG_NULLABLE = 0x23
_TAG_BOOL = 0x2D
_TAG_JSON = 0x30

_PRIMITIVE_TYPE_NAMES = {
    _TAG_NOTHING: "Nothing",
    _TAG_UINT8: "UInt8",
    _TAG_UINT16: "UInt16",
    _TAG_UINT32: "UInt32",
    _TAG_UINT64: "UInt64",
    _TAG_UINT128: "UInt128",
    _TAG_UINT256: "UInt256",
    _TAG_INT8: "Int8",
    _TAG_INT16: "Int16",
    _TAG_INT32: "Int32",
    _TAG_INT64: "Int64",
    _TAG_INT128: "Int128",
    _TAG_INT256: "Int256",
    _TAG_FLOAT32: "Float32",
    _TAG_FLOAT64: "Float64",
    _TAG_DATE: "Date",
    _TAG_DATE32: "Date32",
    _TAG_DATETIME_UTC: "DateTime",
    _TAG_STRING: "String",
    _TAG_BOOL: "Bool",
}


class _BytesReader:
    """Minimal in-memory reader with the slice of the BufferedReader API the
    column readers use, so a SharedVariant blob can be decoded by the very
    same column classes without going near the socket."""

    def __init__(self, data=b""):
        self.reset(data)

    def reset(self, data):
        self.data = data
        self.position = 0

    def read_one(self):
        value = self.data[self.position]
        self.position += 1
        return value

    async def read_bytes(self, length):
        end = self.position + length
        chunk = self.data[self.position : end]
        self.position = end
        return chunk

    async def read_bytes_view(self, length):
        return await self.read_bytes(length)

    async def read_varint(self):
        result = 0
        shift = 0
        while True:
            byte = self.read_one()
            result |= (byte & 0x7F) << shift
            shift += 7
            if byte < 0x80:
                return result

    async def read_str(self, as_bytes=False):
        length = await self.read_varint()
        chunk = await self.read_bytes(length)
        return bytes(chunk) if as_bytes else bytes(chunk).decode()

    async def read_strs(self, n_items, as_bytes=False):
        return [await self.read_str(as_bytes=as_bytes) for _ in range(n_items)]

    async def read_fixed_str(self, length, as_bytes=False):
        chunk = await self.read_bytes(length)
        return bytes(chunk) if as_bytes else bytes(chunk).decode()

    async def read_fixed_strs(self, n_items, length, as_bytes=False):
        return [await self.read_fixed_str(length, as_bytes=as_bytes) for _ in range(n_items)]


def _decode_type_spec(reader):
    """Turn an `encodeDataType` byte stream into a ClickHouse type string.

    A string is produced on purpose: it keeps the handler cache keyed by one
    hash and lets the existing column-by-spec machinery build the reader,
    rather than needing a parallel type AST.
    """
    tag = reader.read_one()

    name = _PRIMITIVE_TYPE_NAMES.get(tag)
    if name is not None:
        return name

    if tag == _TAG_FIXED_STRING:
        return f"FixedString({_sync_varint(reader)})"

    if tag == _TAG_DATETIME_TZ:
        return f"DateTime('{_sync_str(reader)}')"

    if tag == _TAG_DATETIME64_UTC:
        return f"DateTime64({_sync_varint(reader)})"

    if tag == _TAG_DATETIME64_TZ:
        scale = _sync_varint(reader)
        return f"DateTime64({scale}, '{_sync_str(reader)}')"

    if tag == _TAG_NULLABLE:
        return f"Nullable({_decode_type_spec(reader)})"

    if tag == _TAG_ARRAY:
        return f"Array({_decode_type_spec(reader)})"

    if tag == _TAG_TUPLE_UNNAMED:
        size = _sync_varint(reader)
        elements = [_decode_type_spec(reader) for _ in range(size)]
        return "Tuple({})".format(", ".join(elements))

    if tag == _TAG_TUPLE_NAMED:
        size = _sync_varint(reader)
        parts = []
        for _ in range(size):
            element_name = _sync_str(reader)
            parts.append(f"{element_name} {_decode_type_spec(reader)}")
        return "Tuple({})".format(", ".join(parts))

    if tag == _TAG_JSON:
        # None of these parameters change the value layout (every path is
        # dynamic), so the spec collapses to "JSON" once consumed.
        reader.read_one()  # serialization version
        _sync_varint(reader)  # max_dynamic_paths
        reader.read_one()  # max_dynamic_types
        for _ in range(_sync_varint(reader)):  # typed paths
            _sync_str(reader)
            _decode_type_spec(reader)
        for _ in range(_sync_varint(reader)):  # paths to skip
            _sync_str(reader)
        for _ in range(_sync_varint(reader)):  # path regexps to skip
            _sync_str(reader)
        return "JSON"

    raise NotImplementedError(
        f"Cannot decode binary type tag 0x{tag:02x} in a shared JSON value"
    )


def _sync_varint(reader):
    result = 0
    shift = 0
    while True:
        byte = reader.read_one()
        result |= (byte & 0x7F) << shift
        shift += 7
        if byte < 0x80:
            return result


def _sync_str(reader):
    length = _sync_varint(reader)
    end = reader.position + length
    chunk = reader.data[reader.position : end]
    reader.position = end
    return bytes(chunk).decode()


class SharedValueDecoder:
    """Decoder for the `encodeDataType + serializeBinary` payloads carried by
    a SharedVariant's underlying byte-string column.

    One instance per block: the column cache then accumulates across every
    overflow value, both the JSON column's own shared paths and those of
    every nested Dynamic column.
    """

    def __init__(self, column_by_spec_getter):
        self.column_by_spec_getter = column_by_spec_getter
        self._column_cache = {}
        self._reader = _BytesReader()

    def _column_for(self, type_spec):
        column = self._column_cache.get(type_spec)
        if column is None:
            column = self.column_by_spec_getter(type_spec, reader=self._reader)
            self._column_cache[type_spec] = column
        return column

    async def decode(self, blob):
        """Decode one shared value; returns the Python object it encodes.

        Async because the column readers are: nothing here touches the
        socket, the reader is the in-memory blob.
        """
        if not blob:
            return None
        reader = self._reader
        reader.reset(bytes(blob))
        type_spec = _decode_type_spec(reader)
        if type_spec == "Nothing":
            return None
        column = self._column_for(type_spec)
        values = await column.read_data(1)
        return values[0]


def create_dynamic_column(spec, column_by_spec_getter, column_options):
    # `Dynamic(max_types=N)` carries no layout information: the variant list
    # is read from the wire, so the parameters can be ignored.
    return DynamicColumn(column_by_spec_getter, **column_options)

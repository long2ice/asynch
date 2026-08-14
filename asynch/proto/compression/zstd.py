from asynch.proto.compression import BaseCompressor, BaseDecompressor
from asynch.proto.protocol import CompressionMethod, CompressionMethodByte

try:
    # Python 3.14 ships zstd in the standard library; prefer it so the
    # third-party binding is not needed at all on new interpreters. Frames are
    # interchangeable between the two, and both embed the content size, so the
    # one-shot decompress below is safe either way.
    from compression.zstd import compress as _compress
    from compression.zstd import decompress as _decompress
except ImportError:  # pragma: no cover - depends on the interpreter version
    from zstd import compress as _compress  # type: ignore[no-redef]
    from zstd import decompress as _decompress  # type: ignore[no-redef]


class Compressor(BaseCompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD

    def compress_data(self, data):
        return _compress(bytes(data))


class Decompressor(BaseDecompressor):
    method = CompressionMethod.ZSTD
    method_byte = CompressionMethodByte.ZSTD

    def decompress_data(self, data, uncompressed_size):
        return _decompress(data)

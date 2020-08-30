from .lz4 import Compressor as BaseCompressor
from .lz4 import Decompressor as BaseDecompressor


class Compressor(BaseCompressor):
    mode = "high_compression"


class Decompressor(BaseDecompressor):
    pass

import ssl
from typing import Any
from urllib.parse import ParseResult, parse_qs, unquote, urlparse

from asynch.proto.models.enums import ClickhouseScheme, CompressionAlgorithm
from asynch.proto.utils.compat import asbool

_SCHEME_SEPARATOR = "://"

_COMPRESSION_ALGORITHMS: set[str] = {
    CompressionAlgorithm.lz4,
    CompressionAlgorithm.lz4hc,
    CompressionAlgorithm.zstd,
}
_SUPPORTED_SCHEMES: set[str] = {ClickhouseScheme.clickhouse, ClickhouseScheme.clickhouses}
_TIMEOUTS: set[str] = {"connect_timeout", "send_receive_timeout", "sync_request_timeout"}


class DSNError(Exception):
    pass


def parse_dsn(dsn: str) -> dict[str, Any]:
    """Return the client configuration from the given URL.

    The following URL schemes are supported:
    - clickhouse:// - creates a normal TCP socket
    - clickhouses:// - creates an SSL wrapped TCP socket

    Examples::
    - clickhouse://[user:password]@localhost:9000/default
    - clickhouses://[user:password]@localhost:9440/default

    :param dsn str: the DSN string

    :raises DSNError: when parsing fails under the strict mode

    :return: the dictionary of DSN string components
    :rtype: dict[str, Any]
    """

    scheme, sep, rest = dsn.partition(_SCHEME_SEPARATOR)

    if not sep:
        msg = f"no valid scheme separator in the {dsn}"
        raise DSNError(msg)
    if scheme not in _SUPPORTED_SCHEMES:
        msg = f"the scheme {scheme!r} is not in {_SUPPORTED_SCHEMES}"
        raise DSNError(msg)
    if not rest:
        msg = f"nothing to parse after the scheme in the {dsn}"
        raise DSNError(msg)

    settings = {}
    kwargs = {}

    url: ParseResult = urlparse(dsn, scheme=scheme)
    if url.username:
        kwargs["user"] = unquote(url.username)
    if url.password:
        kwargs["password"] = unquote(url.password)
    if url.hostname:
        kwargs["host"] = unquote(url.hostname)
    if url.port:
        kwargs["port"] = url.port

    path = url.path.replace("/", "", 1)
    if path:
        kwargs["database"] = path

    if url.scheme == ClickhouseScheme.clickhouses:
        kwargs["secure"] = True

    for name, value in parse_qs(url.query).items():
        if not value:
            continue
        value = value[0]
        if name == "compression":
            value = value.lower()
            if value in _COMPRESSION_ALGORITHMS:
                kwargs[name] = value
            else:
                kwargs[name] = asbool(value)
        elif name == "secure":
            kwargs[name] = asbool(value)
        elif name == "client_name":
            kwargs[name] = value
        elif name in _TIMEOUTS:
            kwargs[name] = float(value)
        elif name == "compress_block_size":
            kwargs[name] = int(value)
        # ssl
        elif name == "verify":
            kwargs[name] = asbool(value)
        elif name == "ssl_version":
            kwargs[name] = getattr(ssl, value)
        elif name in ["ca_certs", "ciphers"]:
            kwargs[name] = value
        elif name == "alt_hosts":
            kwargs["alt_hosts"] = value
        else:
            settings[name] = value

    if settings:
        kwargs["settings"] = settings

    return kwargs

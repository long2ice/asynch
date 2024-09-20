import ssl
from contextlib import nullcontext as does_not_raise
from typing import Any, ContextManager, Optional

import pytest

from asynch.proto.models.enums import CompressionAlgorithms, Schemes
from asynch.proto.utils.dsn import DSNError, parse_dsn


@pytest.mark.parametrize(
    ("dsn", "ctx", "answer"),
    [
        ("", pytest.raises(DSNError), None),
        ("some_scheme://", pytest.raises(DSNError), None),
        (f"{Schemes.clickhouse}://", pytest.raises(DSNError), None),
        (f"{Schemes.clickhouses}://", pytest.raises(DSNError), None),
        (
            f"{Schemes.clickhouse}://ch@lochost/",
            does_not_raise(),
            {
                "user": "ch",
                "host": "lochost",
            },
        ),
        (
            f"{Schemes.clickhouse}://ch:pwd@lochost/",
            does_not_raise(),
            {
                "user": "ch",
                "password": "pwd",
                "host": "lochost",
            },
        ),
        (
            f"{Schemes.clickhouse}://ch@lochost:4321/",
            does_not_raise(),
            {
                "user": "ch",
                "host": "lochost",
                "port": 4321,
            },
        ),
        (
            f"{Schemes.clickhouse}://ch:pwd@lochost:1234/db",
            does_not_raise(),
            {
                "user": "ch",
                "password": "pwd",
                "host": "lochost",
                "port": 1234,
                "database": "db",
            },
        ),
        (
            "lochost:1234/test",
            pytest.raises(DSNError),
            None,
        ),
        (
            f"{Schemes.clickhouse}://lochost:1234/test",
            does_not_raise(),
            {"host": "lochost", "port": 1234, "database": "test"},
        ),
        (
            f"{Schemes.clickhouse} :// lochost : 1234 / test",
            pytest.raises(DSNError),
            None,
        ),
    ],
)
def test_dsn_basic_credentials(
    dsn: str, ctx: Optional[ContextManager], answer: Optional[dict[str, Any]]
) -> None:
    with ctx:
        result = parse_dsn(dsn=dsn)

        assert result == answer


@pytest.mark.parametrize(
    ("dsn", "query", "answer"),
    [
        (
            f"{Schemes.clickhouses}://ch:pwd@loc:1029/def",
            "verify=true&ssl_version=PROTOCOL_TLSv1&ca_certs=path/to/CA.crt&ciphers=AES&client_name",
            {
                "verify": True,
                "ssl_version": ssl.PROTOCOL_TLSv1,
                "ca_certs": "path/to/CA.crt",
                "ciphers": "AES",
            },
        ),
        (
            f"{Schemes.clickhouse}://ch:pwd@loc:2938/ault",
            (
                "verify=true"
                "&secure=yes"
                "&compression=ZsTD"
                "&client_name=my_ch_client"
                "&compress_block_size=21"
                "&ssl_version=PROTOCOL_TLSv1"
                "&ca_certs=path/to/CA.crt"
                "&ciphers=AES"
                "&intruder=indeed"
                "&empty="
            ),
            {
                "verify": True,
                "secure": True,
                "compression": CompressionAlgorithms.zstd,
                "client_name": "my_ch_client",
                "compress_block_size": 21,
                "ssl_version": ssl.PROTOCOL_TLSv1,
                "ca_certs": "path/to/CA.crt",
                "ciphers": "AES",
                "settings": {"intruder": "indeed"},
            },
        ),
    ],
)
def test_dsn_with_query_fragments(
    dsn: str,
    query: str,
    answer: dict[str, Any],
) -> None:
    url = f"{dsn}?{query}"
    config = parse_dsn(dsn=url)

    for key, value in answer.items():
        if value is None:
            assert config.get(key) is None
            continue
        if key in {
            "ssl_version",
        }:
            assert config[key] is value
            continue
        assert config[key] == value

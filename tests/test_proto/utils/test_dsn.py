import ssl
from contextlib import nullcontext as does_not_raise
from typing import Any, ContextManager, Optional

import pytest

from asynch.proto.models.enums import CompressionAlgorithms
from asynch.proto.utils.dsn import DSNError, parse_dsn


@pytest.mark.parametrize(
    ("dsn", "strict", "ctx", "answer"),
    [
        ("", False, does_not_raise(), {}),
        ("some_scheme://", False, does_not_raise(), {"database": "some_scheme:/"}),
        ("some_scheme://", True, pytest.raises(DSNError), None),
        ("clickhouse://", True, does_not_raise(), {}),
        ("clickhouses://", True, does_not_raise(), {"secure": True}),
        (
            "clickhouse://ch@lochost/",
            False,
            does_not_raise(),
            {
                "user": "ch",
                "host": "lochost",
            },
        ),
        (
            "clickhouse://ch:pwd@lochost/",
            False,
            does_not_raise(),
            {
                "user": "ch",
                "password": "pwd",
                "host": "lochost",
            },
        ),
        (
            "clickhouse://ch@lochost:4321/",
            False,
            does_not_raise(),
            {
                "user": "ch",
                "host": "lochost",
                "port": 4321,
            },
        ),
        (
            "clickhouse://ch:pwd@lochost:1234/db",
            False,
            does_not_raise(),
            {
                "user": "ch",
                "password": "pwd",
                "host": "lochost",
                "port": 1234,
                "database": "db",
            },
        ),
    ],
)
def test_dsn_basic_credentials(
    dsn: str, strict: bool, ctx: Optional[ContextManager], answer: Optional[dict[str, Any]]
) -> None:
    with ctx:
        result = parse_dsn(dsn, strict=strict)

        assert result == answer


@pytest.mark.parametrize(
    ("dsn", "query", "answer"),
    [
        (
            "clickhouses://ch:pwd@loc:1029/def",
            ("verify=true" "&ssl_version=PROTOCOL_TLSv1" "&ca_certs=path/to/CA.crt" "&ciphers=AES"),
            {
                "verify": True,
                "ssl_version": ssl.PROTOCOL_TLSv1,
                "ca_certs": "path/to/CA.crt",
                "ciphers": "AES",
            },
        ),
        (
            "clickhouse://ch:pwd@loc:2938/ault",
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
    config = parse_dsn(url, strict=True)

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

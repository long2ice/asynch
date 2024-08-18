import ssl
from typing import Optional

import pytest

from asynch.connection import Connection

HOST = "192.168.15.103"
PORT = 10000
USER = "ch_user"
PASSWORD = "So~ePa55w0rd"
DATABASE = "db"


def _test_connection_credentials(
    conn: Connection,
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
) -> None:
    __tracebackhide__ = True

    assert conn.host == host
    assert conn.port == port
    assert conn.user == user
    assert conn.password == password
    assert conn.database == database


def _test_connectivity_invariant(
    conn: Connection,
    *,
    is_connected: Optional[bool] = None,
    is_closed: Optional[bool] = None,
) -> None:
    __tracebackhide__ = True

    assert conn.connected is is_connected
    assert conn.closed is is_closed


def test_dsn():
    dsn = f"clickhouse://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    conn = Connection(dsn=dsn)

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)


def test_secure_dsn():
    dsn = (
        f"clickhouses://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        "?verify=true"
        "&ssl_version=PROTOCOL_TLSv1"
        "&ca_certs=path/to/CA.crt"
        "&ciphers=AES"
    )
    conn = Connection(dsn=dsn)

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") == "path/to/CA.crt"
    assert conn._connection.ssl_options.get("ciphers") == "AES"


def test_secure_connection():
    conn = Connection(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        secure=True,
        verify=True,
        ssl_version=ssl.PROTOCOL_TLSv1,
        ca_certs="path/to/CA.crt",
        ciphers="AES",
    )

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") == "path/to/CA.crt"
    assert conn._connection.ssl_options.get("ciphers") == "AES"


def test_secure_connection_check_ssl_context():
    conn = Connection(
        host=HOST,
        port=PORT,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        secure=True,
        ciphers="AES",
        ssl_version=ssl.OP_NO_TLSv1,
    )

    _test_connection_credentials(
        conn, host=HOST, port=PORT, user=USER, password=PASSWORD, database=DATABASE
    )
    _test_connectivity_invariant(conn=conn)
    assert conn._connection.secure_socket
    assert conn._connection.verify
    assert conn._connection.ssl_options.get("ssl_version") is ssl.OP_NO_TLSv1
    assert conn._connection.ssl_options.get("ca_certs") is None
    assert conn._connection.ssl_options.get("ciphers") == "AES"
    ssl_ctx = conn._connection._get_ssl_context()
    assert ssl_ctx
    assert ssl.OP_NO_TLSv1 in ssl_ctx.options


def test_connection_status_offline():
    conn = Connection()
    repstr = f"<Connection object at 0x{id(conn):x}; status: created>"

    assert repr(conn) == repstr
    assert conn.connected is None
    assert conn.closed is None


@pytest.mark.asyncio
async def test_connection_status_online():
    conn = Connection()
    conn_id = id(conn)

    repstr = f"<{conn.__class__.__name__} object at 0x{conn_id:x}"

    try:
        await conn.connect()
        assert repr(conn) == f"{repstr}; status: opened>"
        assert conn.connected
        assert conn.closed is None

        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert conn.connected is False
        assert conn.closed
    finally:
        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert not conn.connected
        assert conn.closed


@pytest.mark.asyncio
async def test_async_context_manager_interface():
    conn = Connection()
    _test_connectivity_invariant(conn=conn)

    async with conn:
        _test_connectivity_invariant(conn=conn, is_connected=True, is_closed=None)
        await conn.ping()

    _test_connectivity_invariant(conn=conn, is_connected=False, is_closed=True)
    try:
        await conn.ping()
    except ConnectionError:
        pass

    async with conn:
        _test_connectivity_invariant(conn=conn, is_connected=True, is_closed=False)
        await conn.ping()


@pytest.mark.asyncio
async def test_connection_ping():
    conn = Connection()  # default

    with pytest.raises(ConnectionError):
        await conn.ping()

    async with conn:
        await conn.ping()

    with pytest.raises(ConnectionError):
        await conn.ping()

    conn = Connection(dsn="clickhouse://inval:9000/non-existent")
    with pytest.raises(ConnectionError):
        await conn.ping()

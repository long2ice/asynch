import asyncio
import ssl
import time

import pytest

from asynch.connection import Connection
from asynch.proto import constants

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
    is_connected: bool = False,
    is_closed: bool = False,
) -> None:
    __tracebackhide__ = True

    assert conn.opened is is_connected
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
    assert not conn.opened
    assert not conn.closed


@pytest.mark.asyncio
async def test_connection_status_online():
    conn = Connection()
    conn_id = id(conn)

    repstr = f"<{conn.__class__.__name__} object at 0x{conn_id:x}"

    try:
        await conn.connect()
        assert repr(conn) == f"{repstr}; status: opened>"
        assert conn.opened
        assert conn.closed is False

        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert conn.opened is False
        assert conn.closed
    finally:
        await conn.close()
        assert repr(conn) == f"{repstr}; status: closed>"
        assert conn.opened is False
        assert conn.closed


@pytest.mark.asyncio
async def test_async_context_manager_interface():
    conn = Connection()
    _test_connectivity_invariant(conn=conn)

    async with conn:
        _test_connectivity_invariant(conn=conn, is_connected=True, is_closed=False)
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


@pytest.mark.asyncio
async def test_connection_cleanup(get_tcp_connections):
    """Test a connection to be properly closed.

    A connection is properly closed if it releases resources,
    especially breaking the TCP channel, leaving no dangling
    connections on a ClickHouse server.

    Plan:
    1. get the number of TCP connections before the test
    2. open N connections, each should execute a query, then closing
    3. assert that the number of TCP connections equals to the initial value
    """

    # get the number of total TCP connections to the ClickHouse
    init_tcps = 0
    conn = Connection()
    async with conn as cn:
        init_tcps = await get_tcp_connections(cn)

    # open-execute-close connections
    for _ in range(100):
        async with Connection() as cn:
            async with cn.cursor() as cur:
                await cur.execute("SELECT 1")
                ret = await cur.fetchone()
                assert ret == (1,)

    final_tcps = 0
    async with conn as cn:
        final_tcps = await get_tcp_connections(cn)

    assert final_tcps == init_tcps


@pytest.mark.asyncio
async def test_connection_close():
    conn = Connection()

    # it does not break
    await conn.close()

    assert not conn.opened
    assert conn.closed

    async with Connection() as conn:
        assert conn.opened

        await conn.close()

        assert not conn.opened
        assert conn.closed


@pytest.mark.parametrize(
    ("kwargs", "expected_port"),
    [
        ({}, constants.DEFAULT_PORT),
        ({"secure": True}, constants.DEFAULT_SECURE_PORT),
        ({"port": 9001}, 9001),
        ({"secure": True, "port": 9500}, 9500),
        ({"dsn": "clickhouse://host/db"}, constants.DEFAULT_PORT),
        ({"dsn": "clickhouses://host/db"}, constants.DEFAULT_SECURE_PORT),
        ({"dsn": "clickhouses://host:9999/db"}, 9999),
    ],
)
def test_secure_default_port(kwargs, expected_port):
    """A secure connection without an explicit port must use 9440, not 9000."""
    conn = Connection(**kwargs)
    assert conn.port == expected_port
    assert conn._connection.hosts[0][1] == expected_port


@pytest.mark.asyncio
async def test_connection_survives_server_exception(conn):
    """A rejected query must not cost the connection.

    Otherwise every SQL error in a pooled application discards a connection.
    """
    from asynch.errors import ServerException

    async with conn.cursor() as cursor:
        with pytest.raises(ServerException):
            await cursor.execute("SELECT this_is_not_a_function(1)")

    assert conn.opened
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 42")
        assert await cursor.fetchone() == (42,)


@pytest.mark.asyncio
async def test_failed_use_does_not_change_tracked_database(conn):
    """A failed `USE` must not move the client's idea of the database."""
    from asynch.errors import ServerException

    before = conn._connection.database
    async with conn.cursor() as cursor:
        with pytest.raises(ServerException):
            await cursor.execute("USE database_that_does_not_exist")

    assert conn._connection.database == before


@pytest.mark.asyncio
async def test_cancelled_query_does_not_wedge_connection(config):
    """A cancelled query must leave the connection usable (or closed), not stuck.

    `asyncio.CancelledError` is a `BaseException`, so it used to skip the
    teardown branch and leave `is_query_executing` set forever — every later
    query then failed with "some records have not been fetched".
    """
    conn = Connection(dsn=config.dsn)
    await conn.connect()
    try:
        async with conn.cursor() as cursor:
            with pytest.raises((asyncio.TimeoutError, TimeoutError)):
                await asyncio.wait_for(cursor.execute("SELECT sleep(3)"), timeout=0.3)

        assert conn._connection.is_query_executing is False

        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 42")
            assert await cursor.fetchone() == (42,)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_cancelled_query_recovers_in_pool(config):
    """The same, through a pool: the connection must not poison the pool."""
    from asynch import Pool

    async with Pool(dsn=config.dsn, minsize=1, maxsize=1) as pool:
        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                with pytest.raises((asyncio.TimeoutError, TimeoutError)):
                    await asyncio.wait_for(cursor.execute("SELECT sleep(3)"), timeout=0.3)

        async with pool.connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 42")
                assert await cursor.fetchone() == (42,)


@pytest.mark.asyncio
async def test_connect_timeout_is_enforced():
    """`connect_timeout` used to be stored and never read.

    A black-holed address would then hang for the OS connect timeout, taking
    alt_hosts failover with it.
    """
    from asynch.errors import NetworkError
    from asynch.proto.connection import Connection as ProtoConnection

    # RFC 5737 TEST-NET-1: guaranteed not to be routed.
    conn = ProtoConnection(host="192.0.2.1", port=9000, connect_timeout=1)
    started = time.monotonic()
    with pytest.raises(NetworkError):
        await asyncio.wait_for(conn.connect(), timeout=15)
    assert time.monotonic() - started < 10

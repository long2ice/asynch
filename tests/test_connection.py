import ssl

from asynch.connection import Connection


def test_dsn():
    dsn = "clickhouse://default:default@127.0.0.1:9000/default"
    conn = Connection(dsn=dsn)
    assert conn.database == "default"
    assert conn.user == "default"
    assert conn.password == "default"
    assert conn.host == "127.0.0.1"
    assert conn.port == 9000


def test_secure_dsn():
    dsn = "clickhouses://default:default@127.0.0.1:9000/default" \
          "?verify=true" \
          "&ssl_version=PROTOCOL_TLSv1" \
          "&ca_certs=path/to/CA.crt" \
          "&ciphers=AES"
    conn = Connection(dsn=dsn)
    assert conn.database == "default"
    assert conn.user == "default"
    assert conn.password == "default"
    assert conn.host == "127.0.0.1"
    assert conn.port == 9000
    assert conn._connection.secure_socket
    assert conn._connection.verify_cert
    assert conn._connection.ssl_options.get('ssl_version') is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get('ca_certs') == 'path/to/CA.crt'
    assert conn._connection.ssl_options.get('ciphers') == 'AES'


def test_secure_connection():
    conn = Connection(
        host='127.0.0.1',
        port=9000,
        user='default',
        password='default',
        database='default',
        secure=True,
        verify=True,
        ssl_version=ssl.PROTOCOL_TLSv1,
        ca_certs='path/to/CA.crt',
        ciphers='AES',
    )
    assert conn.database == "default"
    assert conn.user == "default"
    assert conn.password == "default"
    assert conn.host == "127.0.0.1"
    assert conn.port == 9000
    assert conn._connection.secure_socket
    assert conn._connection.verify_cert
    assert conn._connection.ssl_options.get('ssl_version') is ssl.PROTOCOL_TLSv1
    assert conn._connection.ssl_options.get('ca_certs') == 'path/to/CA.crt'
    assert conn._connection.ssl_options.get('ciphers') == 'AES'


def test_secure_connection_check_ssl_context():
    conn = Connection(
        host='127.0.0.1',
        port=9000,
        user='default',
        password='default',
        database='default',
        secure=True,
        ciphers='AES',
        ssl_version=ssl.OP_NO_TLSv1,
    )
    assert conn.database == "default"
    assert conn.user == "default"
    assert conn.password == "default"
    assert conn.host == "127.0.0.1"
    assert conn.port == 9000
    assert conn._connection.secure_socket
    assert conn._connection.verify_cert
    assert conn._connection.ssl_options.get('ssl_version') is ssl.OP_NO_TLSv1
    assert conn._connection.ssl_options.get('ca_certs') is None
    assert conn._connection.ssl_options.get('ciphers') == 'AES'
    ssl_ctx = conn._connection._get_ssl_context()
    assert ssl_ctx is not None
    assert ssl.OP_NO_TLSv1 in ssl_ctx.options

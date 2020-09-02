from asynch.connection import Connection


def test_dsn():
    dsn = "clickhouse://default:default@127.0.0.1:9000/default"
    conn = Connection(dsn=dsn)
    assert conn.database == "default"
    assert conn.user == "default"
    assert conn.password == "default"
    assert conn.host == "127.0.0.1"
    assert conn.port == 9000

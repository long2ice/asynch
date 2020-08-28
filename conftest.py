import asyncio

import pytest

from asynch.proto.connection import Connection

conn = Connection()


@pytest.yield_fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    res = policy.new_event_loop()
    asyncio.set_event_loop(res)
    res._close = res.close
    res.close = lambda: None

    yield res

    res._close()


@pytest.fixture(scope="session", autouse=True)
def initialize_tests(event_loop):
    event_loop.run_until_complete(conn.connect())

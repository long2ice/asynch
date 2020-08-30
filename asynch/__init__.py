from asynch.connection import Connection


async def connect(dsn: str = None, host=None, port=None, user=None, password=None, database=None):
    return Connection(dsn)


async def create_pool():
    pass

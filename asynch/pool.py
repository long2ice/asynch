from asynch.connection import Connection


class Pool:
    def __init__(self):
        pass

    async def _create_new_connection(self):
        pass

    async def release(self, connection: Connection):
        pass

    async def acquire(self) -> Connection:
        pass

    async def _initialize(self):
        pass

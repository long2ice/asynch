from asynch import errors


class Connection:
    async def close(self):
        pass

    async def commit(self):
        raise errors.NotSupportedError

    async def rollback(self):
        raise errors.NotSupportedError

    async def cursor(self):
        pass

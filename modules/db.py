import logging
import os
from typing import Any, Optional

import asyncpg


log = logging.getLogger(__name__)


class Database:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        dsn = os.getenv("POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("POSTGRES_DSN is not set")
        self.pool = await asyncpg.create_pool(dsn)
        log.info("PostgreSQL pool created")

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None
            log.info("PostgreSQL pool closed")

    async def fetch(self, query: str, *args: Any):
        assert self.pool
        return await self.pool.fetch(query, *args)

    async def fetchrow(self, query: str, *args: Any):
        assert self.pool
        return await self.pool.fetchrow(query, *args)

    async def fetchval(self, query: str, *args: Any):
        assert self.pool
        return await self.pool.fetchval(query, *args)

    async def execute(self, query: str, *args: Any):
        assert self.pool
        return await self.pool.execute(query, *args)


db = Database()

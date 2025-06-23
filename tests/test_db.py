import os
import pytest
import pytest_asyncio

from modules.db import db

DSN = "postgresql:///postgres?user=root"


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    os.environ["POSTGRES_DSN"] = DSN
    await db.connect()
    await db.execute("DROP TABLE IF EXISTS items")
    await db.execute("CREATE TABLE items(id serial PRIMARY KEY, name text, value int)")
    yield
    await db.execute("DROP TABLE items")
    await db.close()


@pytest.mark.asyncio
async def test_connect_close():
    assert db.pool is not None
    await db.close()
    assert db.pool is None
    await db.connect()


@pytest.mark.asyncio
async def test_execute_and_fetch():
    await db.execute("INSERT INTO items(name, value) VALUES($1, $2)", "a", 1)
    rows = await db.fetch("SELECT * FROM items")
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_fetchrow():
    await db.execute("INSERT INTO items(name, value) VALUES($1, $2)", "b", 2)
    row = await db.fetchrow("SELECT name FROM items WHERE value=$1", 2)
    assert row["name"] == "b"


@pytest.mark.asyncio
async def test_fetchval():
    await db.execute("INSERT INTO items(name, value) VALUES($1, $2)", "c", 3)
    val = await db.fetchval("SELECT value FROM items WHERE name=$1", "c")
    assert val == 3

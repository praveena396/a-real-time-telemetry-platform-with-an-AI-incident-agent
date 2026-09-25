import os
import uuid

import pytest
import pytest_asyncio

from app.storage.base import MemoryStore

TEST_DB = os.environ.get("TEST_DATABASE_URL")


@pytest.fixture
def memory_store():
    return MemoryStore()


@pytest_asyncio.fixture
async def pg_store():
    """A PgStore on a fresh schema. Skipped unless TEST_DATABASE_URL is set."""
    if not TEST_DB:
        pytest.skip("TEST_DATABASE_URL not set")
    import asyncpg

    from app.storage.postgres import PgStore

    schema = f"t_{uuid.uuid4().hex[:8]}"
    admin = await asyncpg.connect(TEST_DB)
    await admin.execute(f"CREATE SCHEMA {schema}")
    dsn = TEST_DB + ("&" if "?" in TEST_DB else "?") + f"options=-csearch_path%3D{schema},public"
    store = await PgStore.connect(dsn, min_size=1, max_size=4)
    try:
        yield store
    finally:
        await store.close()
        await admin.execute(f"DROP SCHEMA {schema} CASCADE")
        await admin.close()

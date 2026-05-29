import asyncio
import logging

import asyncpg

from .settings import int_env, pg_dsn

logger = logging.getLogger(__name__)


async def create_pool_with_retry() -> asyncpg.Pool:
    attempts = int_env("DB_CONNECT_ATTEMPTS", 30)
    delay = int_env("DB_CONNECT_DELAY_SECONDS", 2)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return await asyncpg.create_pool(
                dsn=pg_dsn(),
                min_size=1,
                max_size=int_env("DB_POOL_MAX_SIZE", 5),
                command_timeout=30,
            )
        except Exception as exc: 
            last_error = exc
            logger.warning("PostgreSQL is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
            await asyncio.sleep(delay)

    raise RuntimeError("Could not connect to PostgreSQL") from last_error


async def ping_postgres(pool: asyncpg.Pool) -> bool:
    try:
        async with pool.acquire() as conn:
            value = await conn.fetchval("select 1")
        return value == 1
    except Exception:
        logger.exception("PostgreSQL readiness check failed")
        return False


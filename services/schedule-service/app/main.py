import asyncio
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone

import asyncpg
import grpc
import redis.asyncio as redis
import uvicorn
from fastapi import FastAPI, Response

import schedule_pb2 as pb2
import schedule_pb2_grpc as pb2_grpc
from laundry_common.db import create_pool_with_retry, ping_postgres
from laundry_common.settings import env, int_env, redis_url
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("schedule-service")


def _slot_response(row) -> pb2.SlotResponse:
    return pb2.SlotResponse(
        id=str(row["id"]),
        machine_id=str(row["machine_id"]),
        starts_at=row["starts_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        ends_at=row["ends_at"].astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        status=row["status"],
        booking_id=str(row["booking_id"]) if row["booking_id"] else "",
    )


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def _parse_time(value: str) -> time:
    return time.fromisoformat(value)


def _day_range(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


async def create_redis_with_retry() -> redis.Redis:
    attempts = int_env("REDIS_CONNECT_ATTEMPTS", 30)
    delay = int_env("REDIS_CONNECT_DELAY_SECONDS", 2)
    client = redis.from_url(redis_url(), decode_responses=True)
    for attempt in range(1, attempts + 1):
        try:
            await client.ping()
            return client
        except Exception as exc:  # pragma: no cover - startup resilience
            logger.warning("Redis is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect to Redis")


class ScheduleServicer(pb2_grpc.ScheduleServiceServicer):
    def __init__(self, pool: asyncpg.Pool, redis_client: redis.Redis):
        self.pool = pool
        self.redis = redis_client
        self.lock_ttl = int_env("SLOT_LOCK_TTL_SECONDS", 300)

    def _lock_key(self, slot_id: str) -> str:
        return f"slot_lock:{slot_id}"

    async def GenerateSlots(self, request, context):
        if request.slot_duration_minutes <= 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Slot duration must be positive")
        try:
            day = _parse_day(request.date)
            start_time = _parse_time(request.start_time)
            end_time = _parse_time(request.end_time)
            machine_id = uuid.UUID(request.machine_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid date, time or machine_id")

        start_at = datetime.combine(day, start_time, tzinfo=timezone.utc)
        end_at = datetime.combine(day, end_time, tzinfo=timezone.utc)
        if start_at >= end_at:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "start_time must be before end_time")

        created = 0
        current = start_at
        duration = timedelta(minutes=request.slot_duration_minutes)
        async with self.pool.acquire() as conn:
            while current + duration <= end_at:
                result = await conn.execute(
                    """
                    insert into schedule.slots(id, machine_id, starts_at, ends_at, status)
                    values($1, $2, $3, $4, 'FREE')
                    on conflict(machine_id, starts_at, ends_at) do nothing
                    """,
                    uuid.uuid4(),
                    machine_id,
                    current,
                    current + duration,
                )
                if result.endswith("1"):
                    created += 1
                current += duration

        return pb2.GenerateSlotsResponse(created_count=created)

    async def ListAvailableSlots(self, request, context):
        try:
            day = _parse_day(request.date)
            machine_id = uuid.UUID(request.machine_id) if request.machine_id else None
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid date or machine_id")

        start, end = _day_range(day)
        async with self.pool.acquire() as conn:
            if machine_id:
                rows = await conn.fetch(
                    """
                    select id, machine_id, starts_at, ends_at, status, booking_id
                    from schedule.slots
                    where machine_id = $1
                      and starts_at >= $2
                      and starts_at < $3
                      and status = 'FREE'
                    order by starts_at
                    """,
                    machine_id,
                    start,
                    end,
                )
            else:
                rows = await conn.fetch(
                    """
                    select id, machine_id, starts_at, ends_at, status, booking_id
                    from schedule.slots
                    where starts_at >= $1
                      and starts_at < $2
                      and status = 'FREE'
                    order by starts_at
                    """,
                    start,
                    end,
                )

        available = []
        for row in rows:
            locked = await self.redis.exists(self._lock_key(str(row["id"])))
            if not locked:
                available.append(_slot_response(row))
        return pb2.SlotListResponse(items=available)

    async def LockSlot(self, request, context):
        try:
            slot_id = uuid.UUID(request.slot_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid slot_id")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select id, machine_id, starts_at, ends_at, status, booking_id
                from schedule.slots
                where id = $1
                """,
                slot_id,
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Slot not found")
        if row["status"] != "FREE":
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Slot is not free")

        locked = await self.redis.set(
            self._lock_key(request.slot_id),
            request.booking_id,
            ex=self.lock_ttl,
            nx=True,
        )
        if not locked:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, "Slot is already locked")
        return _slot_response(row)

    async def ConfirmSlot(self, request, context):
        try:
            slot_id = uuid.UUID(request.slot_id)
            booking_id = uuid.UUID(request.booking_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid slot_id or booking_id")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update schedule.slots
                set status = 'BOOKED', booking_id = $2, updated_at = now()
                where id = $1
                returning id, machine_id, starts_at, ends_at, status, booking_id
                """,
                slot_id,
                booking_id,
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Slot not found")
        await self.redis.delete(self._lock_key(request.slot_id))
        return _slot_response(row)

    async def ReleaseSlot(self, request, context):
        try:
            slot_id = uuid.UUID(request.slot_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid slot_id")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update schedule.slots
                set status = 'FREE', booking_id = null, updated_at = now()
                where id = $1
                returning id, machine_id, starts_at, ends_at, status, booking_id
                """,
                slot_id,
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Slot not found")
        await self.redis.delete(self._lock_key(request.slot_id))
        return _slot_response(row)


async def start_grpc_server(app: FastAPI) -> grpc.aio.Server:
    server = grpc.aio.server()
    pb2_grpc.add_ScheduleServiceServicer_to_server(
        ScheduleServicer(app.state.pool, app.state.redis),
        server,
    )
    server.add_insecure_port(f"[::]:{int_env('GRPC_PORT', 50051)}")
    await server.start()
    logger.info("schedule-service gRPC started")
    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("schedule-service")
    app.state.ready = False
    app.state.pool = await create_pool_with_retry()
    app.state.redis = await create_redis_with_retry()
    app.state.grpc_server = await start_grpc_server(app)
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False
        await app.state.grpc_server.stop(grace=10)
        await app.state.redis.aclose()
        await app.state.pool.close()


app = FastAPI(title="schedule-service", lifespan=lifespan)
instrument_fastapi(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "schedule-service"}


@app.get("/ready")
async def ready(response: Response):
    postgres_ready = getattr(app.state, "ready", False) and await ping_postgres(app.state.pool)
    try:
        redis_ready = await app.state.redis.ping()
    except Exception:
        redis_ready = False
    if not postgres_ready or not redis_ready:
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8000))


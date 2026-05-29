import asyncio
import json
import logging
import random
import uuid
from contextlib import asynccontextmanager
from datetime import date

import asyncpg
import grpc
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Response

import event_worker_pb2 as pb2
import event_worker_pb2_grpc as pb2_grpc
from laundry_common.db import create_pool_with_retry, ping_postgres
from laundry_common.events import event_from_bytes, event_to_bytes, new_event
from laundry_common.settings import env, float_env, int_env, kafka_servers
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("event-worker-service")


async def create_producer_with_retry() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=kafka_servers())
    attempts = int_env("KAFKA_CONNECT_ATTEMPTS", 30)
    delay = int_env("KAFKA_CONNECT_DELAY_SECONDS", 2)
    for attempt in range(1, attempts + 1):
        try:
            await producer.start()
            return producer
        except Exception as exc:  # pragma: no cover
            logger.warning("Kafka producer is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect Kafka producer")


async def create_consumer_with_retry(topics: list[str]) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=kafka_servers(),
        group_id=env("EVENT_WORKER_CONSUMER_GROUP", "event-worker-service"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    attempts = int_env("KAFKA_CONNECT_ATTEMPTS", 30)
    delay = int_env("KAFKA_CONNECT_DELAY_SECONDS", 2)
    for attempt in range(1, attempts + 1):
        try:
            await consumer.start()
            return consumer
        except Exception as exc:  # pragma: no cover
            logger.warning("Kafka consumer is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect Kafka consumer")


def _uuid_or_none(value: str | None):
    if not value:
        return None
    try:
        return uuid.UUID(value)
    except ValueError:
        return None


async def mark_processed(conn: asyncpg.Connection, event_id: str) -> bool:
    try:
        row = await conn.fetchrow(
            """
            insert into events.processed_events(event_id)
            values($1)
            on conflict do nothing
            returning event_id
            """,
            uuid.UUID(event_id),
        )
        return row is not None
    except ValueError:
        return True


async def save_audit(conn: asyncpg.Connection, event: dict) -> None:
    payload = event.get("payload", {})
    aggregate_id = _uuid_or_none(payload.get("booking_id") or payload.get("payment_id"))
    user_id = _uuid_or_none(payload.get("user_id"))
    await conn.execute(
        """
        insert into events.audit_log(id, event_id, event_type, aggregate_id, user_id, payload)
        values($1, $2, $3, $4, $5, $6::jsonb)
        on conflict do nothing
        """,
        uuid.uuid4(),
        _uuid_or_none(event.get("event_id")),
        event.get("event_type", "unknown"),
        aggregate_id,
        user_id,
        json.dumps(event, ensure_ascii=False),
    )


async def update_analytics(conn: asyncpg.Connection, event: dict) -> None:
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    amount = float(payload.get("amount") or payload.get("price") or 0)
    today = date.today()

    await conn.execute(
        """
        insert into events.analytics_daily(day)
        values($1)
        on conflict(day) do nothing
        """,
        today,
    )

    if event_type == "booking.created":
        await conn.execute(
            "update events.analytics_daily set bookings_count = bookings_count + 1 where day = $1",
            today,
        )
    elif event_type == "booking.cancelled":
        await conn.execute(
            "update events.analytics_daily set cancelled_count = cancelled_count + 1 where day = $1",
            today,
        )
    elif event_type == "payment.succeeded":
        await conn.execute(
            """
            update events.analytics_daily
            set payment_success_count = payment_success_count + 1,
                revenue = revenue + $2
            where day = $1
            """,
            today,
            amount,
        )
    elif event_type == "payment.failed":
        await conn.execute(
            "update events.analytics_daily set payment_failed_count = payment_failed_count + 1 where day = $1",
            today,
        )


async def publish_mock_payment(app: FastAPI, event: dict) -> None:
    payload = event.get("payload", {})
    booking_id = payload.get("booking_id")
    if not booking_id:
        return

    await asyncio.sleep(random.randint(1, 3))
    forced = env("PAYMENT_FORCE_RESULT", "").strip().lower()
    if forced in {"success", "succeeded"}:
        succeeded = True
    elif forced in {"fail", "failed"}:
        succeeded = False
    else:
        succeeded = random.random() < float_env("PAYMENT_SUCCESS_RATE", 0.8)

    payment_id = str(uuid.uuid4())
    if succeeded:
        payment_event = new_event(
            "payment.succeeded",
            "event-worker-service",
            {
                "payment_id": payment_id,
                "booking_id": booking_id,
                "user_id": payload.get("user_id", ""),
                "amount": float(payload.get("price") or 100),
            },
            request_id=event.get("request_id", ""),
            trace_id=event.get("trace_id", ""),
        )
    else:
        payment_event = new_event(
            "payment.failed",
            "event-worker-service",
            {
                "payment_id": payment_id,
                "booking_id": booking_id,
                "user_id": payload.get("user_id", ""),
                "reason": "mock_declined",
            },
            request_id=event.get("request_id", ""),
            trace_id=event.get("trace_id", ""),
        )

    await app.state.producer.send_and_wait(
        env("PAYMENT_EVENTS_TOPIC", "payment-events"),
        value=event_to_bytes(payment_event),
        key=booking_id.encode("utf-8"),
    )
    logger.info("Mock payment published: %s for booking %s", payment_event["event_type"], booking_id)


async def handle_event(app: FastAPI, event: dict) -> None:
    event_id = event.get("event_id")
    async with app.state.pool.acquire() as conn:
        async with conn.transaction():
            if event_id and not await mark_processed(conn, event_id):
                return
            await save_audit(conn, event)
            await update_analytics(conn, event)

    logger.info("Mock notification created for %s", event.get("event_type"))
    if event.get("event_type") == "booking.created":
        await publish_mock_payment(app, event)


async def consume_events(app: FastAPI) -> None:
    try:
        async for message in app.state.consumer:
            try:
                await handle_event(app, event_from_bytes(message.value))
            except Exception:
                logger.exception("Could not process event-worker message")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Event worker consumer stopped unexpectedly")


def _audit_record(row) -> pb2.AuditRecord:
    payload = row["payload"] if isinstance(row["payload"], str) else json.dumps(row["payload"], ensure_ascii=False, default=str)
    return pb2.AuditRecord(
        id=str(row["id"]),
        event_type=row["event_type"],
        aggregate_id=str(row["aggregate_id"]) if row["aggregate_id"] else "",
        user_id=str(row["user_id"]) if row["user_id"] else "",
        payload=payload,
        created_at=row["created_at"].isoformat(),
    )


class EventWorkerServicer(pb2_grpc.EventWorkerServiceServicer):
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def GetAuditLog(self, request, context):
        limit = request.limit if request.limit > 0 else 100
        offset = request.offset if request.offset > 0 else 0
        async with self.pool.acquire() as conn:
            if request.event_type:
                rows = await conn.fetch(
                    """
                    select id, event_type, aggregate_id, user_id, payload, created_at
                    from events.audit_log
                    where event_type = $1
                    order by created_at desc
                    limit $2 offset $3
                    """,
                    request.event_type,
                    limit,
                    offset,
                )
            else:
                rows = await conn.fetch(
                    """
                    select id, event_type, aggregate_id, user_id, payload, created_at
                    from events.audit_log
                    order by created_at desc
                    limit $1 offset $2
                    """,
                    limit,
                    offset,
                )
        return pb2.AuditLogResponse(items=[_audit_record(row) for row in rows])

    async def GetAnalyticsSummary(self, request, context):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select
                    coalesce(sum(bookings_count), 0)::int as bookings_count,
                    coalesce(sum(cancelled_count), 0)::int as cancelled_count,
                    coalesce(sum(payment_success_count), 0)::int as payment_success_count,
                    coalesce(sum(payment_failed_count), 0)::int as payment_failed_count,
                    coalesce(sum(revenue), 0)::numeric as revenue
                from events.analytics_daily
                """
            )
        return pb2.AnalyticsSummaryResponse(
            bookings_count=row["bookings_count"],
            cancelled_count=row["cancelled_count"],
            payment_success_count=row["payment_success_count"],
            payment_failed_count=row["payment_failed_count"],
            revenue=float(row["revenue"]),
        )


async def start_grpc_server(app: FastAPI) -> grpc.aio.Server:
    server = grpc.aio.server()
    pb2_grpc.add_EventWorkerServiceServicer_to_server(EventWorkerServicer(app.state.pool), server)
    server.add_insecure_port(f"[::]:{int_env('GRPC_PORT', 50051)}")
    await server.start()
    logger.info("event-worker-service gRPC started")
    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("event-worker-service")
    app.state.ready = False
    app.state.pool = await create_pool_with_retry()
    app.state.producer = await create_producer_with_retry()
    app.state.consumer = await create_consumer_with_retry(
        [env("BOOKING_EVENTS_TOPIC", "booking-events"), env("PAYMENT_EVENTS_TOPIC", "payment-events")]
    )
    app.state.consumer_task = asyncio.create_task(consume_events(app))
    app.state.grpc_server = await start_grpc_server(app)
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False
        app.state.consumer_task.cancel()
        await asyncio.gather(app.state.consumer_task, return_exceptions=True)
        await app.state.consumer.stop()
        await app.state.producer.stop()
        await app.state.grpc_server.stop(grace=10)
        await app.state.pool.close()


app = FastAPI(title="event-worker-service", lifespan=lifespan)
instrument_fastapi(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "event-worker-service"}


@app.get("/ready")
async def ready(response: Response):
    if not getattr(app.state, "ready", False) or not await ping_postgres(app.state.pool):
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8000))

import asyncio
import logging
import uuid
from contextlib import asynccontextmanager

import asyncpg
import grpc
import uvicorn
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Response

import booking_pb2 as pb2
import booking_pb2_grpc as pb2_grpc
import machine_pb2
import machine_pb2_grpc
import schedule_pb2
import schedule_pb2_grpc
from laundry_common.db import create_pool_with_retry, ping_postgres
from laundry_common.events import event_from_bytes, event_to_bytes, new_event
from laundry_common.grpc_utils import grpc_status_to_http, metadata_value
from laundry_common.settings import env, int_env, kafka_servers
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("booking-service")


def _booking_response(row) -> pb2.BookingResponse:
    return pb2.BookingResponse(
        id=str(row["id"]),
        user_id=str(row["user_id"]),
        machine_id=str(row["machine_id"]),
        slot_id=str(row["slot_id"]),
        status=row["status"],
        price=float(row["price"]),
        service_version=env("SERVICE_VERSION", "v1"),
    )


async def create_producer_with_retry() -> AIOKafkaProducer:
    producer = AIOKafkaProducer(bootstrap_servers=kafka_servers())
    attempts = int_env("KAFKA_CONNECT_ATTEMPTS", 30)
    delay = int_env("KAFKA_CONNECT_DELAY_SECONDS", 2)
    for attempt in range(1, attempts + 1):
        try:
            await producer.start()
            return producer
        except Exception as exc:  # pragma: no cover - startup resilience
            logger.warning("Kafka producer is not ready yet, attempt %s/%s: %s", attempt, attempts, exc)
            await asyncio.sleep(delay)
    raise RuntimeError("Could not connect Kafka producer")


async def create_consumer_with_retry(topic: str) -> AIOKafkaConsumer:
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=kafka_servers(),
        group_id=env("PAYMENT_CONSUMER_GROUP", "booking-service"),
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


class BookingServicer(pb2_grpc.BookingServiceServicer):
    def __init__(
        self,
        pool: asyncpg.Pool,
        producer: AIOKafkaProducer,
        machine_stub: machine_pb2_grpc.MachineServiceStub,
        schedule_stub: schedule_pb2_grpc.ScheduleServiceStub,
    ):
        self.pool = pool
        self.producer = producer
        self.machine_stub = machine_stub
        self.schedule_stub = schedule_stub
        self.booking_topic = env("BOOKING_EVENTS_TOPIC", "booking-events")

    async def CreateBooking(self, request, context):
        try:
            user_id = uuid.UUID(request.user_id)
            machine_id = uuid.UUID(request.machine_id)
            slot_id = uuid.UUID(request.slot_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid user_id, machine_id or slot_id")

        if request.idempotency_key:
            async with self.pool.acquire() as conn:
                existing = await conn.fetchrow(
                    """
                    select id, user_id, machine_id, slot_id, status, price
                    from booking.bookings
                    where user_id = $1 and idempotency_key = $2
                    """,
                    user_id,
                    request.idempotency_key,
                )
            if existing:
                return _booking_response(existing)

        try:
            machine = await self.machine_stub.GetMachine(
                machine_pb2.GetMachineRequest(machine_id=request.machine_id),
                timeout=5,
            )
        except grpc.aio.AioRpcError as exc:
            await context.abort(exc.code(), exc.details())
        if machine.status != "ACTIVE":
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Machine is not active")

        booking_id = uuid.uuid4()
        locked = False
        try:
            await self.schedule_stub.LockSlot(
                schedule_pb2.LockSlotRequest(slot_id=request.slot_id, booking_id=str(booking_id)),
                timeout=5,
            )
            locked = True

            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    insert into booking.bookings(
                        id, user_id, machine_id, slot_id, status, price, idempotency_key
                    )
                    values($1, $2, $3, $4, 'PENDING_PAYMENT', 100, $5)
                    returning id, user_id, machine_id, slot_id, status, price
                    """,
                    booking_id,
                    user_id,
                    machine_id,
                    slot_id,
                    request.idempotency_key or None,
                )

            event = new_event(
                "booking.created",
                "booking-service",
                {
                    "booking_id": str(booking_id),
                    "user_id": str(user_id),
                    "machine_id": str(machine_id),
                    "slot_id": str(slot_id),
                    "price": 100,
                },
                request_id=metadata_value(context, "x-request-id"),
            )
            await self.producer.send_and_wait(
                self.booking_topic,
                value=event_to_bytes(event),
                key=str(booking_id).encode("utf-8"),
            )
            return _booking_response(row)
        except grpc.aio.AioRpcError as exc:
            await context.abort(exc.code(), exc.details())
        except asyncpg.UniqueViolationError:
            if locked:
                try:
                    await self.schedule_stub.ReleaseSlot(
                        schedule_pb2.ReleaseSlotRequest(slot_id=request.slot_id, booking_id=str(booking_id)),
                        timeout=5,
                    )
                except Exception:
                    logger.exception("Could not release slot after idempotency conflict")
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, "Booking already exists for idempotency key")
        except Exception as exc:
            logger.exception("CreateBooking failed")
            if locked:
                try:
                    await self.schedule_stub.ReleaseSlot(
                        schedule_pb2.ReleaseSlotRequest(slot_id=request.slot_id, booking_id=str(booking_id)),
                        timeout=5,
                    )
                except Exception:
                    logger.exception("Could not release slot after failed booking creation")
            await context.abort(grpc.StatusCode.UNAVAILABLE, f"Could not create booking: {exc}")

    async def CancelBooking(self, request, context):
        try:
            booking_id = uuid.UUID(request.booking_id)
            user_id = uuid.UUID(request.user_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid booking_id or user_id")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select id, user_id, machine_id, slot_id, status, price
                from booking.bookings
                where id = $1
                """,
                booking_id,
            )
            if row is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, "Booking not found")
            if request.role != "ADMIN" and row["user_id"] != user_id:
                await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Booking belongs to another user")
            if row["status"] == "CANCELLED":
                return _booking_response(row)

            updated = await conn.fetchrow(
                """
                update booking.bookings
                set status = 'CANCELLED', updated_at = now()
                where id = $1
                returning id, user_id, machine_id, slot_id, status, price
                """,
                booking_id,
            )

        try:
            await self.schedule_stub.ReleaseSlot(
                schedule_pb2.ReleaseSlotRequest(slot_id=str(updated["slot_id"]), booking_id=str(booking_id)),
                timeout=5,
            )
        except grpc.aio.AioRpcError:
            logger.exception("Could not release slot during cancellation")

        event = new_event(
            "booking.cancelled",
            "booking-service",
            {
                "booking_id": str(booking_id),
                "user_id": str(updated["user_id"]),
                "reason": "user_cancelled",
            },
            request_id=metadata_value(context, "x-request-id"),
        )
        await self.producer.send_and_wait(
            self.booking_topic,
            value=event_to_bytes(event),
            key=str(booking_id).encode("utf-8"),
        )
        return _booking_response(updated)

    async def ListUserBookings(self, request, context):
        try:
            user_id = uuid.UUID(request.user_id)
        except ValueError:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid user_id")
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select id, user_id, machine_id, slot_id, status, price
                from booking.bookings
                where user_id = $1
                order by created_at desc
                """,
                user_id,
            )
        return pb2.BookingListResponse(items=[_booking_response(row) for row in rows])

    async def ListAllBookings(self, request, context):
        limit = request.limit if request.limit > 0 else 100
        offset = request.offset if request.offset > 0 else 0
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                select id, user_id, machine_id, slot_id, status, price
                from booking.bookings
                order by created_at desc
                limit $1 offset $2
                """,
                limit,
                offset,
            )
        return pb2.BookingListResponse(items=[_booking_response(row) for row in rows])


async def handle_payment_event(app: FastAPI, event: dict) -> None:
    event_type = event.get("event_type")
    payload = event.get("payload", {})
    if event_type not in {"payment.succeeded", "payment.failed"}:
        return

    booking_id = payload.get("booking_id")
    if not booking_id:
        return

    async with app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, slot_id, status
            from booking.bookings
            where id = $1
            """,
            uuid.UUID(booking_id),
        )
    if row is None or row["status"] != "PENDING_PAYMENT":
        return

    if event_type == "payment.succeeded":
        try:
            await app.state.schedule_stub.ConfirmSlot(
                schedule_pb2.ConfirmSlotRequest(slot_id=str(row["slot_id"]), booking_id=booking_id),
                timeout=5,
            )
            async with app.state.pool.acquire() as conn:
                await conn.execute(
                    """
                    update booking.bookings
                    set status = 'CONFIRMED', updated_at = now()
                    where id = $1 and status = 'PENDING_PAYMENT'
                    """,
                    uuid.UUID(booking_id),
                )
            logger.info("Booking confirmed: %s", booking_id)
        except Exception:
            logger.exception("Could not confirm booking %s", booking_id)
    else:
        try:
            await app.state.schedule_stub.ReleaseSlot(
                schedule_pb2.ReleaseSlotRequest(slot_id=str(row["slot_id"]), booking_id=booking_id),
                timeout=5,
            )
            async with app.state.pool.acquire() as conn:
                await conn.execute(
                    """
                    update booking.bookings
                    set status = 'PAYMENT_FAILED', updated_at = now()
                    where id = $1 and status = 'PENDING_PAYMENT'
                    """,
                    uuid.UUID(booking_id),
                )
            logger.info("Booking payment failed: %s", booking_id)
        except Exception:
            logger.exception("Could not fail booking %s", booking_id)


async def consume_payments(app: FastAPI) -> None:
    try:
        async for message in app.state.consumer:
            try:
                await handle_payment_event(app, event_from_bytes(message.value))
            except Exception:
                logger.exception("Could not process payment event")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Payment consumer stopped unexpectedly")


async def start_grpc_server(app: FastAPI) -> grpc.aio.Server:
    server = grpc.aio.server()
    pb2_grpc.add_BookingServiceServicer_to_server(
        BookingServicer(app.state.pool, app.state.producer, app.state.machine_stub, app.state.schedule_stub),
        server,
    )
    server.add_insecure_port(f"[::]:{int_env('GRPC_PORT', 50051)}")
    await server.start()
    logger.info("booking-service gRPC started")
    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("booking-service")
    app.state.ready = False
    app.state.pool = await create_pool_with_retry()
    app.state.machine_channel = grpc.aio.insecure_channel(env("MACHINE_GRPC_TARGET", "machine-service:50051"))
    app.state.schedule_channel = grpc.aio.insecure_channel(env("SCHEDULE_GRPC_TARGET", "schedule-service:50051"))
    app.state.machine_stub = machine_pb2_grpc.MachineServiceStub(app.state.machine_channel)
    app.state.schedule_stub = schedule_pb2_grpc.ScheduleServiceStub(app.state.schedule_channel)
    app.state.producer = await create_producer_with_retry()
    app.state.consumer = await create_consumer_with_retry(env("PAYMENT_EVENTS_TOPIC", "payment-events"))
    app.state.consumer_task = asyncio.create_task(consume_payments(app))
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
        await app.state.machine_channel.close()
        await app.state.schedule_channel.close()
        await app.state.pool.close()


app = FastAPI(title="booking-service", lifespan=lifespan)
instrument_fastapi(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "booking-service", "version": env("SERVICE_VERSION", "v1")}


@app.get("/ready")
async def ready(response: Response):
    if not getattr(app.state, "ready", False) or not await ping_postgres(app.state.pool):
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8000))

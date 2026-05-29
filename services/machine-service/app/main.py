import logging
import uuid
from contextlib import asynccontextmanager

import asyncpg
import grpc
import uvicorn
from fastapi import FastAPI, Response

import machine_pb2 as pb2
import machine_pb2_grpc as pb2_grpc
from laundry_common.db import create_pool_with_retry, ping_postgres
from laundry_common.settings import env, int_env
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("machine-service")

ALLOWED_STATUSES = {"ACTIVE", "MAINTENANCE", "DISABLED"}


def _machine_response(row) -> pb2.MachineResponse:
    return pb2.MachineResponse(
        id=str(row["id"]),
        name=row["name"],
        location=row["location"],
        capacity_kg=float(row["capacity_kg"]),
        status=row["status"],
    )


class MachineServicer(pb2_grpc.MachineServiceServicer):
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def CreateMachine(self, request, context):
        name = request.name.strip()
        location = request.location.strip()
        if not name or not location:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Name and location are required")
        if request.capacity_kg <= 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Capacity must be positive")

        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                insert into machine.washing_machines(id, name, location, capacity_kg, status)
                values($1, $2, $3, $4, 'ACTIVE')
                returning id, name, location, capacity_kg, status
                """,
                uuid.uuid4(),
                name,
                location,
                request.capacity_kg,
            )
        return _machine_response(row)

    async def UpdateMachineStatus(self, request, context):
        status = request.status.strip().upper()
        if status not in ALLOWED_STATUSES:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid machine status")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update machine.washing_machines
                set status = $2, updated_at = now()
                where id = $1
                returning id, name, location, capacity_kg, status
                """,
                uuid.UUID(request.machine_id),
                status,
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Machine not found")
        return _machine_response(row)

    async def ListMachines(self, request, context):
        status = request.status.strip().upper()
        async with self.pool.acquire() as conn:
            if status:
                rows = await conn.fetch(
                    """
                    select id, name, location, capacity_kg, status
                    from machine.washing_machines
                    where status = $1
                    order by created_at desc
                    """,
                    status,
                )
            else:
                rows = await conn.fetch(
                    """
                    select id, name, location, capacity_kg, status
                    from machine.washing_machines
                    order by created_at desc
                    """
                )
        return pb2.MachineListResponse(items=[_machine_response(row) for row in rows])

    async def GetMachine(self, request, context):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select id, name, location, capacity_kg, status
                from machine.washing_machines
                where id = $1
                """,
                uuid.UUID(request.machine_id),
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Machine not found")
        return _machine_response(row)


async def seed_demo_machine(pool: asyncpg.Pool) -> None:
    if env("SEED_DEMO_DATA", "true").lower() != "true":
        return
    async with pool.acquire() as conn:
        exists = await conn.fetchval("select count(*) from machine.washing_machines")
        if exists == 0:
            await conn.execute(
                """
                insert into machine.washing_machines(id, name, location, capacity_kg, status)
                values($1, 'Machine 1', 'Dormitory 1', 6, 'ACTIVE')
                """,
                uuid.uuid4(),
            )
            logger.info("Demo washing machine created")


async def start_grpc_server(app: FastAPI) -> grpc.aio.Server:
    server = grpc.aio.server()
    pb2_grpc.add_MachineServiceServicer_to_server(MachineServicer(app.state.pool), server)
    server.add_insecure_port(f"[::]:{int_env('GRPC_PORT', 50051)}")
    await server.start()
    logger.info("machine-service gRPC started")
    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("machine-service")
    app.state.ready = False
    app.state.pool = await create_pool_with_retry()
    await seed_demo_machine(app.state.pool)
    app.state.grpc_server = await start_grpc_server(app)
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False
        await app.state.grpc_server.stop(grace=10)
        await app.state.pool.close()


app = FastAPI(title="machine-service", lifespan=lifespan)
instrument_fastapi(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "machine-service"}


@app.get("/ready")
async def ready(response: Response):
    if not getattr(app.state, "ready", False) or not await ping_postgres(app.state.pool):
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8000))


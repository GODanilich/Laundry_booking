import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg
import grpc
import uvicorn
from fastapi import FastAPI, Response

import identity_pb2 as pb2
import identity_pb2_grpc as pb2_grpc
from laundry_common.db import create_pool_with_retry, ping_postgres
from laundry_common.security import (
    create_access_token,
    create_refresh_token,
    hash_password,
    hash_token,
    verify_password,
)
from laundry_common.settings import env, int_env
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("identity-service")


def _user_response(row) -> pb2.UserResponse:
    return pb2.UserResponse(
        id=str(row["id"]),
        email=row["email"],
        full_name=row["full_name"],
        role=row["role"],
    )


class IdentityServicer(pb2_grpc.IdentityServiceServicer):
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def Register(self, request, context):
        email = request.email.strip().lower()
        if not email or "@" not in email:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid email")
        if len(request.password) < 6:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Password must contain at least 6 chars")
        full_name = request.full_name.strip() or email

        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    """
                    insert into identity.users(id, email, password_hash, full_name, role)
                    values($1, $2, $3, $4, 'USER')
                    returning id, email, full_name, role
                    """,
                    uuid.uuid4(),
                    email,
                    hash_password(request.password),
                    full_name,
                )
            except asyncpg.UniqueViolationError:
                await context.abort(grpc.StatusCode.ALREADY_EXISTS, "User already exists")

        return _user_response(row)

    async def Login(self, request, context):
        email = request.email.strip().lower()
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select id, email, password_hash, full_name, role
                from identity.users
                where email = $1
                """,
                email,
            )
            if row is None or not verify_password(request.password, row["password_hash"]):
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid email or password")

            refresh_token = create_refresh_token()
            await conn.execute(
                """
                insert into identity.refresh_tokens(id, user_id, token_hash, expires_at)
                values($1, $2, $3, $4)
                """,
                uuid.uuid4(),
                row["id"],
                hash_token(refresh_token),
                datetime.now(timezone.utc) + timedelta(days=int_env("JWT_REFRESH_TTL_DAYS", 7)),
            )

        return pb2.TokenResponse(
            access_token=create_access_token(str(row["id"]), row["email"], row["role"]),
            refresh_token=refresh_token,
            token_type="Bearer",
            expires_in=int_env("JWT_ACCESS_TTL_SECONDS", 900),
        )

    async def Refresh(self, request, context):
        token_hash = hash_token(request.refresh_token)
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                select u.id, u.email, u.role
                from identity.refresh_tokens rt
                join identity.users u on u.id = rt.user_id
                where rt.token_hash = $1
                  and rt.revoked_at is null
                  and rt.expires_at > now()
                """,
                token_hash,
            )
            if row is None:
                await context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid refresh token")

            await conn.execute(
                "update identity.refresh_tokens set revoked_at = now() where token_hash = $1",
                token_hash,
            )
            refresh_token = create_refresh_token()
            await conn.execute(
                """
                insert into identity.refresh_tokens(id, user_id, token_hash, expires_at)
                values($1, $2, $3, $4)
                """,
                uuid.uuid4(),
                row["id"],
                hash_token(refresh_token),
                datetime.now(timezone.utc) + timedelta(days=int_env("JWT_REFRESH_TTL_DAYS", 7)),
            )

        return pb2.TokenResponse(
            access_token=create_access_token(str(row["id"]), row["email"], row["role"]),
            refresh_token=refresh_token,
            token_type="Bearer",
            expires_in=int_env("JWT_ACCESS_TTL_SECONDS", 900),
        )

    async def GetProfile(self, request, context):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "select id, email, full_name, role from identity.users where id = $1",
                uuid.UUID(request.user_id),
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "User not found")
        return _user_response(row)

    async def UpdateProfile(self, request, context):
        full_name = request.full_name.strip()
        if not full_name:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Full name is required")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                update identity.users
                set full_name = $2, updated_at = now()
                where id = $1
                returning id, email, full_name, role
                """,
                uuid.UUID(request.user_id),
                full_name,
            )
        if row is None:
            await context.abort(grpc.StatusCode.NOT_FOUND, "User not found")
        return _user_response(row)


async def ensure_admin(pool: asyncpg.Pool) -> None:
    email = env("ADMIN_EMAIL", "admin@example.com").strip().lower()
    password = env("ADMIN_PASSWORD", "Admin123")
    full_name = env("ADMIN_FULL_NAME", "System Admin")
    async with pool.acquire() as conn:
        row = await conn.fetchrow("select id from identity.users where email = $1", email)
        if row is None:
            await conn.execute(
                """
                insert into identity.users(id, email, password_hash, full_name, role)
                values($1, $2, $3, $4, 'ADMIN')
                """,
                uuid.uuid4(),
                email,
                hash_password(password),
                full_name,
            )
            logger.info("Default admin created: %s", email)
        else:
            await conn.execute("update identity.users set role = 'ADMIN' where email = $1", email)


async def start_grpc_server(app: FastAPI) -> grpc.aio.Server:
    server = grpc.aio.server()
    pb2_grpc.add_IdentityServiceServicer_to_server(IdentityServicer(app.state.pool), server)
    server.add_insecure_port(f"[::]:{int_env('GRPC_PORT', 50051)}")
    await server.start()
    logger.info("identity-service gRPC started")
    return server


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("identity-service")
    app.state.ready = False
    app.state.pool = await create_pool_with_retry()
    await ensure_admin(app.state.pool)
    app.state.grpc_server = await start_grpc_server(app)
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False
        await app.state.grpc_server.stop(grace=10)
        await app.state.pool.close()


app = FastAPI(title="identity-service", lifespan=lifespan)
instrument_fastapi(app)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "identity-service"}


@app.get("/ready")
async def ready(response: Response):
    if not getattr(app.state, "ready", False) or not await ping_postgres(app.state.pool):
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8000))


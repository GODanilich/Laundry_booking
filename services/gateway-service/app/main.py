import logging
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, Any

import grpc
import jwt
import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import booking_pb2
import booking_pb2_grpc
import event_worker_pb2
import event_worker_pb2_grpc
import identity_pb2
import identity_pb2_grpc
import machine_pb2
import machine_pb2_grpc
import schedule_pb2
import schedule_pb2_grpc
from laundry_common.grpc_utils import grpc_status_to_http, request_metadata
from laundry_common.security import decode_access_token
from laundry_common.settings import env, int_env
from laundry_common.tracing import instrument_fastapi, setup_tracing

logging.basicConfig(level=env("LOG_LEVEL", "INFO"))
logger = logging.getLogger("gateway-service")


class RegisterIn(BaseModel):
    email: str
    password: str
    full_name: str


class LoginIn(BaseModel):
    email: str
    password: str


class RefreshIn(BaseModel):
    refresh_token: str


class UpdateProfileIn(BaseModel):
    full_name: str


class CreateMachineIn(BaseModel):
    name: str
    location: str
    capacity_kg: float


class UpdateMachineStatusIn(BaseModel):
    status: str


class GenerateSlotsIn(BaseModel):
    machine_id: str
    date: str
    slot_duration_minutes: int = 60
    start_time: str = "08:00"
    end_time: str = "22:00"


class CreateBookingIn(BaseModel):
    machine_id: str
    slot_id: str


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "")


def api_error(status_code: int, code: str, message: str, request: Request | None = None) -> HTTPException:
    return HTTPException(
        status_code=status_code,
        detail={
            "code": code,
            "message": message,
            "request_id": _request_id(request) if request else "",
        },
    )


def user_to_dict(user) -> dict[str, Any]:
    return {
        "id": user.id,
        "email": user.email,
        "full_name": user.full_name,
        "role": user.role,
    }


def token_to_dict(token) -> dict[str, Any]:
    return {
        "access_token": token.access_token,
        "refresh_token": token.refresh_token,
        "token_type": token.token_type,
        "expires_in": token.expires_in,
    }


def machine_to_dict(machine) -> dict[str, Any]:
    return {
        "id": machine.id,
        "name": machine.name,
        "location": machine.location,
        "capacity_kg": machine.capacity_kg,
        "status": machine.status,
    }


def slot_to_dict(slot) -> dict[str, Any]:
    return {
        "id": slot.id,
        "machine_id": slot.machine_id,
        "starts_at": slot.starts_at,
        "ends_at": slot.ends_at,
        "status": slot.status,
        "booking_id": slot.booking_id or None,
    }


def booking_to_dict(booking) -> dict[str, Any]:
    return {
        "id": booking.id,
        "user_id": booking.user_id,
        "machine_id": booking.machine_id,
        "slot_id": booking.slot_id,
        "status": booking.status,
        "price": booking.price,
        "service_version": booking.service_version,
    }


def audit_to_dict(record) -> dict[str, Any]:
    return {
        "id": record.id,
        "event_type": record.event_type,
        "aggregate_id": record.aggregate_id or None,
        "user_id": record.user_id or None,
        "payload": record.payload,
        "created_at": record.created_at,
    }


async def grpc_call(request: Request, call):
    try:
        return await call
    except grpc.aio.AioRpcError as exc:
        raise api_error(grpc_status_to_http(exc.code()), exc.code().name, exc.details(), request) from exc


def grpc_metadata(request: Request, user: dict[str, Any] | None = None) -> list[tuple[str, str]]:
    return request_metadata(
        request_id=_request_id(request),
        user_id=user["id"] if user else "",
        role=user["role"] if user else "",
    )


async def current_user(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise api_error(401, "UNAUTHENTICATED", "Authorization header is required", request)
    token = authorization.split(" ", 1)[1]
    try:
        payload = decode_access_token(token)
    except jwt.PyJWTError as exc:
        raise api_error(401, "UNAUTHENTICATED", "Invalid access token", request) from exc
    return {
        "id": payload["sub"],
        "email": payload.get("email", ""),
        "role": payload.get("role", "USER"),
    }


async def admin_user(
    request: Request,
    user: Annotated[dict[str, Any], Depends(current_user)],
) -> dict[str, Any]:
    if user.get("role") != "ADMIN":
        raise api_error(403, "PERMISSION_DENIED", "Admin role is required", request)
    return user


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_tracing("gateway-service")
    app.state.ready = False
    app.state.identity_channel = grpc.aio.insecure_channel(env("IDENTITY_GRPC_TARGET", "identity-service:50051"))
    app.state.machine_channel = grpc.aio.insecure_channel(env("MACHINE_GRPC_TARGET", "machine-service:50051"))
    app.state.schedule_channel = grpc.aio.insecure_channel(env("SCHEDULE_GRPC_TARGET", "schedule-service:50051"))
    app.state.booking_channel = grpc.aio.insecure_channel(env("BOOKING_GRPC_TARGET", "booking-service:50051"))
    app.state.event_worker_channel = grpc.aio.insecure_channel(
        env("EVENT_WORKER_GRPC_TARGET", "event-worker-service:50051")
    )
    app.state.identity = identity_pb2_grpc.IdentityServiceStub(app.state.identity_channel)
    app.state.machine = machine_pb2_grpc.MachineServiceStub(app.state.machine_channel)
    app.state.schedule = schedule_pb2_grpc.ScheduleServiceStub(app.state.schedule_channel)
    app.state.booking = booking_pb2_grpc.BookingServiceStub(app.state.booking_channel)
    app.state.event_worker = event_worker_pb2_grpc.EventWorkerServiceStub(app.state.event_worker_channel)
    app.state.ready = True
    try:
        yield
    finally:
        app.state.ready = False
        await app.state.identity_channel.close()
        await app.state.machine_channel.close()
        await app.state.schedule_channel.close()
        await app.state.booking_channel.close()
        await app.state.event_worker_channel.close()


app = FastAPI(title="Laundry Booking Gateway", version="0.1.0", lifespan=lifespan)
instrument_fastapi(app)


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request.state.request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    response = await call_next(request)
    response.headers["X-Request-ID"] = request.state.request_id
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "HTTP_ERROR", "message": str(exc.detail)}
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": detail.get("code", "HTTP_ERROR"),
                "message": detail.get("message", "Request failed"),
                "request_id": detail.get("request_id") or _request_id(request),
            }
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={
            "error": {
                "code": "VALIDATION_ERROR",
                "message": str(exc.errors()),
                "request_id": _request_id(request),
            }
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway-service"}


@app.get("/ready")
async def ready(response: Response):
    if not getattr(app.state, "ready", False):
        response.status_code = 503
        return {"status": "not_ready"}
    return {"status": "ready"}


@app.post("/api/v1/auth/register", status_code=201)
async def register(request: Request, body: RegisterIn):
    result = await grpc_call(
        request,
        app.state.identity.Register(
            identity_pb2.RegisterRequest(email=body.email, password=body.password, full_name=body.full_name),
            metadata=grpc_metadata(request),
            timeout=5,
        ),
    )
    return user_to_dict(result)


@app.post("/api/v1/auth/login")
async def login(request: Request, body: LoginIn):
    result = await grpc_call(
        request,
        app.state.identity.Login(
            identity_pb2.LoginRequest(email=body.email, password=body.password),
            metadata=grpc_metadata(request),
            timeout=5,
        ),
    )
    return token_to_dict(result)


@app.post("/api/v1/auth/refresh")
async def refresh(request: Request, body: RefreshIn):
    result = await grpc_call(
        request,
        app.state.identity.Refresh(
            identity_pb2.RefreshRequest(refresh_token=body.refresh_token),
            metadata=grpc_metadata(request),
            timeout=5,
        ),
    )
    return token_to_dict(result)


@app.get("/api/v1/users/me")
async def get_me(request: Request, user: Annotated[dict[str, Any], Depends(current_user)]):
    result = await grpc_call(
        request,
        app.state.identity.GetProfile(
            identity_pb2.GetProfileRequest(user_id=user["id"]),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return user_to_dict(result)


@app.patch("/api/v1/users/me")
async def update_me(
    request: Request,
    body: UpdateProfileIn,
    user: Annotated[dict[str, Any], Depends(current_user)],
):
    result = await grpc_call(
        request,
        app.state.identity.UpdateProfile(
            identity_pb2.UpdateProfileRequest(user_id=user["id"], full_name=body.full_name),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return user_to_dict(result)


@app.get("/api/v1/machines")
async def list_machines(request: Request, status: str = ""):
    result = await grpc_call(
        request,
        app.state.machine.ListMachines(
            machine_pb2.ListMachinesRequest(status=status),
            metadata=grpc_metadata(request),
            timeout=5,
        ),
    )
    return {"items": [machine_to_dict(item) for item in result.items]}


@app.post("/api/v1/admin/machines", status_code=201)
async def create_machine(
    request: Request,
    body: CreateMachineIn,
    user: Annotated[dict[str, Any], Depends(admin_user)],
):
    result = await grpc_call(
        request,
        app.state.machine.CreateMachine(
            machine_pb2.CreateMachineRequest(
                name=body.name,
                location=body.location,
                capacity_kg=body.capacity_kg,
            ),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return machine_to_dict(result)


@app.patch("/api/v1/admin/machines/{machine_id}/status")
async def update_machine_status(
    request: Request,
    machine_id: str,
    body: UpdateMachineStatusIn,
    user: Annotated[dict[str, Any], Depends(admin_user)],
):
    result = await grpc_call(
        request,
        app.state.machine.UpdateMachineStatus(
            machine_pb2.UpdateMachineStatusRequest(machine_id=machine_id, status=body.status),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return machine_to_dict(result)


@app.post("/api/v1/admin/slots/generate", status_code=201)
async def generate_slots(
    request: Request,
    body: GenerateSlotsIn,
    user: Annotated[dict[str, Any], Depends(admin_user)],
):
    result = await grpc_call(
        request,
        app.state.schedule.GenerateSlots(
            schedule_pb2.GenerateSlotsRequest(
                machine_id=body.machine_id,
                date=body.date,
                slot_duration_minutes=body.slot_duration_minutes,
                start_time=body.start_time,
                end_time=body.end_time,
            ),
            metadata=grpc_metadata(request, user),
            timeout=10,
        ),
    )
    return {"created_count": result.created_count}


@app.get("/api/v1/slots")
async def list_slots(
    request: Request,
    date: Annotated[str, Query(description="Date in YYYY-MM-DD format")],
    machine_id: str = "",
):
    result = await grpc_call(
        request,
        app.state.schedule.ListAvailableSlots(
            schedule_pb2.ListAvailableSlotsRequest(machine_id=machine_id, date=date),
            metadata=grpc_metadata(request),
            timeout=5,
        ),
    )
    return {"items": [slot_to_dict(item) for item in result.items]}


@app.post("/api/v1/bookings", status_code=201)
async def create_booking(
    request: Request,
    body: CreateBookingIn,
    user: Annotated[dict[str, Any], Depends(current_user)],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
):
    result = await grpc_call(
        request,
        app.state.booking.CreateBooking(
            booking_pb2.CreateBookingRequest(
                user_id=user["id"],
                role=user["role"],
                machine_id=body.machine_id,
                slot_id=body.slot_id,
                idempotency_key=idempotency_key or "",
            ),
            metadata=grpc_metadata(request, user),
            timeout=10,
        ),
    )
    return booking_to_dict(result)


@app.delete("/api/v1/bookings/{booking_id}")
async def cancel_booking(
    request: Request,
    booking_id: str,
    user: Annotated[dict[str, Any], Depends(current_user)],
):
    result = await grpc_call(
        request,
        app.state.booking.CancelBooking(
            booking_pb2.CancelBookingRequest(booking_id=booking_id, user_id=user["id"], role=user["role"]),
            metadata=grpc_metadata(request, user),
            timeout=10,
        ),
    )
    return booking_to_dict(result)


@app.get("/api/v1/bookings/my")
async def my_bookings(request: Request, user: Annotated[dict[str, Any], Depends(current_user)]):
    result = await grpc_call(
        request,
        app.state.booking.ListUserBookings(
            booking_pb2.ListUserBookingsRequest(user_id=user["id"]),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return {"items": [booking_to_dict(item) for item in result.items]}


@app.get("/api/v1/admin/bookings")
async def all_bookings(
    request: Request,
    user: Annotated[dict[str, Any], Depends(admin_user)],
    limit: int = 100,
    offset: int = 0,
):
    result = await grpc_call(
        request,
        app.state.booking.ListAllBookings(
            booking_pb2.ListAllBookingsRequest(limit=limit, offset=offset),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return {"items": [booking_to_dict(item) for item in result.items]}


@app.get("/api/v1/admin/audit")
async def audit_log(
    request: Request,
    user: Annotated[dict[str, Any], Depends(admin_user)],
    event_type: str = "",
    limit: int = 100,
    offset: int = 0,
):
    result = await grpc_call(
        request,
        app.state.event_worker.GetAuditLog(
            event_worker_pb2.GetAuditLogRequest(event_type=event_type, limit=limit, offset=offset),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return {"items": [audit_to_dict(item) for item in result.items]}


@app.get("/api/v1/admin/analytics/summary")
async def analytics_summary(
    request: Request,
    user: Annotated[dict[str, Any], Depends(admin_user)],
):
    result = await grpc_call(
        request,
        app.state.event_worker.GetAnalyticsSummary(
            event_worker_pb2.GetAnalyticsSummaryRequest(),
            metadata=grpc_metadata(request, user),
            timeout=5,
        ),
    )
    return {
        "bookings_count": result.bookings_count,
        "cancelled_count": result.cancelled_count,
        "payment_success_count": result.payment_success_count,
        "payment_failed_count": result.payment_failed_count,
        "revenue": result.revenue,
    }


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=int_env("HTTP_PORT", 8080))

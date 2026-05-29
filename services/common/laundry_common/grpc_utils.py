from collections.abc import Iterable

import grpc


def metadata_value(context: grpc.aio.ServicerContext, key: str, default: str = "") -> str:
    metadata: Iterable[tuple[str, str]] = context.invocation_metadata() or []
    lowered = key.lower()
    for item_key, item_value in metadata:
        if item_key.lower() == lowered:
            return item_value
    return default


def request_metadata(request_id: str = "", user_id: str = "", role: str = "") -> list[tuple[str, str]]:
    metadata: list[tuple[str, str]] = []
    if request_id:
        metadata.append(("x-request-id", request_id))
    if user_id:
        metadata.append(("x-user-id", user_id))
    if role:
        metadata.append(("x-user-role", role))
    return metadata


def grpc_status_to_http(code: grpc.StatusCode) -> int:
    mapping = {
        grpc.StatusCode.INVALID_ARGUMENT: 400,
        grpc.StatusCode.UNAUTHENTICATED: 401,
        grpc.StatusCode.PERMISSION_DENIED: 403,
        grpc.StatusCode.NOT_FOUND: 404,
        grpc.StatusCode.ALREADY_EXISTS: 409,
        grpc.StatusCode.FAILED_PRECONDITION: 409,
        grpc.StatusCode.UNAVAILABLE: 503,
    }
    return mapping.get(code, 500)


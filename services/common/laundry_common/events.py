import json
import uuid
from datetime import datetime, timezone
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def new_event(
    event_type: str,
    producer: str,
    payload: dict[str, Any],
    request_id: str = "",
    trace_id: str = "",
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": utc_now_iso(),
        "trace_id": trace_id,
        "request_id": request_id,
        "producer": producer,
        "payload": payload,
    }


def event_to_bytes(event: dict[str, Any]) -> bytes:
    return json.dumps(event, ensure_ascii=False, default=str).encode("utf-8")


def event_from_bytes(value: bytes) -> dict[str, Any]:
    return json.loads(value.decode("utf-8"))


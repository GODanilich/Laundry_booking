#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
TODAY="$(date +%F)"
USER_EMAIL="user-$(date -u +%s)@example.com"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

json_get() {
  python3 -c '
import json
import sys

data = json.load(sys.stdin)
path = sys.argv[1].split(".")
value = data
for part in path:
    if part.isdigit():
        value = value[int(part)]
    else:
        value = value[part]
print(value)
' "$1"
}

api() {
  local method="$1"
  local path="$2"
  local body="${3:-}"
  local auth_header="${4:-}"

  local args=(
    --fail
    --silent
    --show-error
    --request "$method"
    "${BASE_URL}${path}"
  )

  if [[ -n "$auth_header" ]]; then
    args+=(--header "$auth_header")
  fi

  if [[ -n "$body" ]]; then
    args+=(--header "Content-Type: application/json" --data "$body")
  fi

  curl "${args[@]}"
}

require_command curl
require_command python3

echo "Checking gateway health..."
api GET "/health"
echo

echo "Logging in as admin..."
admin_tokens="$(api POST "/api/v1/auth/login" '{"email":"admin@example.com","password":"Admin123"}')"
admin_access_token="$(printf '%s' "$admin_tokens" | json_get "access_token")"
admin_auth="Authorization: Bearer ${admin_access_token}"

echo "Loading machines..."
machines="$(api GET "/api/v1/machines")"
machine_count="$(printf '%s' "$machines" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))')"
if [[ "$machine_count" -eq 0 ]]; then
  echo "No machines found, creating demo machine..."
  machine="$(api POST "/api/v1/admin/machines" '{"name":"Machine 1","location":"Dormitory 1","capacity_kg":6}' "$admin_auth")"
else
  machine="$(printf '%s' "$machines" | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["items"][0]))')"
fi

machine_id="$(printf '%s' "$machine" | json_get "id")"
echo "Using machine ${machine_id}"

echo "Generating slots for ${TODAY}..."
api POST "/api/v1/admin/slots/generate" "{\"machine_id\":\"${machine_id}\",\"date\":\"${TODAY}\",\"slot_duration_minutes\":60,\"start_time\":\"08:00\",\"end_time\":\"12:00\"}" "$admin_auth"
echo

echo "Registering test user ${USER_EMAIL}..."
api POST "/api/v1/auth/register" "{\"email\":\"${USER_EMAIL}\",\"password\":\"Password123\",\"full_name\":\"Smoke Test User\"}"
echo

echo "Logging in as test user..."
user_tokens="$(api POST "/api/v1/auth/login" "{\"email\":\"${USER_EMAIL}\",\"password\":\"Password123\"}")"
user_access_token="$(printf '%s' "$user_tokens" | json_get "access_token")"
user_auth="Authorization: Bearer ${user_access_token}"

echo "Loading available slots..."
slots="$(api GET "/api/v1/slots?machine_id=${machine_id}&date=${TODAY}")"
slot_count="$(printf '%s' "$slots" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["items"]))')"
if [[ "$slot_count" -eq 0 ]]; then
  echo "No available slots for smoke test" >&2
  exit 1
fi
slot_id="$(printf '%s' "$slots" | json_get "items.0.id")"

echo "Creating booking for slot ${slot_id}..."
booking="$(api POST "/api/v1/bookings" "{\"machine_id\":\"${machine_id}\",\"slot_id\":\"${slot_id}\"}" "$user_auth")"
printf '%s\n' "$booking" | python3 -m json.tool

echo "Waiting for mock payment..."
sleep 6

echo "User bookings:"
api GET "/api/v1/bookings/my" "" "$user_auth" | python3 -m json.tool

echo "Analytics summary:"
api GET "/api/v1/admin/analytics/summary" "" "$admin_auth" | python3 -m json.tool

echo "Recent audit records:"
api GET "/api/v1/admin/audit?limit=10" "" "$admin_auth" | python3 -m json.tool

echo "Smoke test completed."

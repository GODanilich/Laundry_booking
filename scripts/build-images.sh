#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

services=(
  "gateway-service"
  "identity-service"
  "machine-service"
  "schedule-service"
  "booking-service"
  "event-worker-service"
)

for service in "${services[@]}"; do
  echo "Building laundry/${service}:local"
  docker build \
    -f "services/${service}/Dockerfile" \
    -t "laundry/${service}:local" \
    .
done

echo "All service images built."

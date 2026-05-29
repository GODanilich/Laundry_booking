#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building booking-service v2 image..."
docker build \
  -f "services/booking-service/Dockerfile" \
  -t "laundry/booking-service:v2" \
  .

echo "Updating only booking-service with start-first rolling update..."
docker service update \
  --image "laundry/booking-service:v2" \
  --env-rm SERVICE_VERSION \
  --env-add SERVICE_VERSION=v2 \
  --update-order start-first \
  --update-parallelism 1 \
  laundry_booking-service

echo "Watch update status with:"
echo "docker service ps laundry_booking-service"

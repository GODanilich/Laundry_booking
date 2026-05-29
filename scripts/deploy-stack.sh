#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

swarm_state="$(docker info --format '{{.Swarm.LocalNodeState}}' 2>/dev/null || true)"
if [[ "$swarm_state" != "active" ]]; then
  echo "Initializing Docker Swarm..."
  docker swarm init
fi

echo "Deploying laundry stack..."
docker stack deploy --prune -c deployments/docker-stack.yml laundry

echo "Stack deployment requested. Check status with:"
echo "docker stack services laundry"

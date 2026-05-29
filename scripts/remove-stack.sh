#!/usr/bin/env bash
set -euo pipefail

docker stack rm laundry

echo "Stack removal requested. Volumes are kept by Docker."

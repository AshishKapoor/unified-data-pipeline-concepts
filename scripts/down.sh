#!/usr/bin/env bash
# Tear down everything (core + Kafka overlay). Pass --volumes to also drop named volumes.
set -euo pipefail
cd "$(dirname "$0")/.."

EXTRA=""
if [ "${1:-}" = "--volumes" ]; then
  EXTRA="--volumes"
  echo "Removing named volumes (artifacts, checkpoints, savepoints)..."
fi

docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml down $EXTRA
echo "Stack stopped."

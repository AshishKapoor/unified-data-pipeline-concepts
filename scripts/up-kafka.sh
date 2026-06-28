#!/usr/bin/env bash
# Bring up the core stack PLUS the Kafka overlay (needed for the opt-in cross-language path in
# Ch 15 / Ch 16). The default demos run without this; bring it up only for the real Kafka path.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f docker/.env ]; then set -a && . docker/.env && set +a; fi
FLINK_UI_PORT="${FLINK_UI_PORT:-8081}"

echo "Starting core stack + Kafka overlay..."
docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml up -d --build

FLINK_URL="http://localhost:${FLINK_UI_PORT}" ./scripts/wait-for-flink.sh

echo "Waiting for Kafka..."
until docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml \
        exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 3
done
echo "Kafka is up. Next: ./scripts/seed-kafka.sh"

#!/usr/bin/env bash
# Bring up the core stack PLUS the Kafka overlay (needed for Ch 15 and Ch 16).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting core stack + Kafka overlay..."
docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml up -d --build

./scripts/wait-for-flink.sh

echo "Waiting for Kafka..."
until docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml \
        exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; do
  sleep 3
done
echo "Kafka is up. Next: ./scripts/seed-kafka.sh"

#!/usr/bin/env bash
# Create the Ch 15/16 topics and produce a handful of sample events.
# Requires the Kafka overlay to be up (./scripts/up-kafka.sh).
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f docker/docker-compose.yml -f docker/docker-compose.kafka.yml)

create_topic() {
  echo "Creating topic: $1"
  "${COMPOSE[@]}" exec -T kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$1" --partitions 2 --replication-factor 1
}

create_topic "clicks-in"
create_topic "counts-out"

echo "Producing sample events to clicks-in..."
"${COMPOSE[@]}" exec -T kafka bash -lc '
cat <<EOF | kafka-console-producer --bootstrap-server localhost:9092 --topic clicks-in --property "parse.key=true" --property "key.separator=:"
user-1:click
user-2:click
user-1:click
user-3:click
user-2:click
user-1:click
EOF'

echo "Done. Topics: clicks-in (source), counts-out (sink). Run: ./scripts/submit.sh ch15"

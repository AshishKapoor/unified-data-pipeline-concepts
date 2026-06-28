#!/usr/bin/env bash
# Poll the Flink JobManager REST API until the cluster reports a registered TaskManager.
set -euo pipefail

FLINK_URL="${FLINK_URL:-http://localhost:8081}"
TIMEOUT="${TIMEOUT:-120}"

echo "Waiting for Flink at ${FLINK_URL} (timeout ${TIMEOUT}s)..."
deadline=$(( $(date +%s) + TIMEOUT ))
while true; do
  if overview=$(curl -sf "${FLINK_URL}/overview" 2>/dev/null); then
    tms=$(printf '%s' "$overview" | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')
    if [ "${tms:-0}" -ge 1 ]; then
      echo "Flink is up: ${tms} taskmanager(s) registered."
      exit 0
    fi
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "ERROR: Flink did not become ready within ${TIMEOUT}s." >&2
    exit 1
  fi
  sleep 3
done

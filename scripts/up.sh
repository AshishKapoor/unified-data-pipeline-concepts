#!/usr/bin/env bash
# Bring up the CORE stack (Flink + Beam job server + worker pool + NestJS API + submitter).
set -euo pipefail
cd "$(dirname "$0")/.."

# Respect host-port overrides from docker/.env (if present) so the readiness check + the printed
# URLs use the ports you actually published.
if [ -f docker/.env ]; then set -a && . docker/.env && set +a; fi
FLINK_UI_PORT="${FLINK_UI_PORT:-8081}"
API_PORT="${API_PORT:-3000}"

echo "Starting core stack..."
docker compose -f docker/docker-compose.yml up -d --build

FLINK_URL="http://localhost:${FLINK_UI_PORT}" ./scripts/wait-for-flink.sh

cat <<EOF

Core stack is up.
  Interactive docs : http://localhost:${API_PORT}/docs
  Swagger API      : http://localhost:${API_PORT}/docs/api
  Flink Web UI     : http://localhost:${FLINK_UI_PORT}

Smoke-test the portability path:   ./scripts/submit.sh wordcount
Run chapter 1:                     ./scripts/submit.sh ch01
EOF

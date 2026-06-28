#!/usr/bin/env bash
# Bring up the CORE stack (Flink + Beam job server + worker pool + NestJS API + submitter).
set -euo pipefail
cd "$(dirname "$0")/.."

echo "Starting core stack..."
docker compose -f docker/docker-compose.yml up -d --build

./scripts/wait-for-flink.sh

cat <<'EOF'

Core stack is up.
  Interactive docs : http://localhost:3000/docs
  Swagger API      : http://localhost:3000/docs/api
  Flink Web UI     : http://localhost:8081

Smoke-test the portability path:   ./scripts/submit.sh wordcount
Run chapter 1:                     ./scripts/submit.sh ch01
EOF

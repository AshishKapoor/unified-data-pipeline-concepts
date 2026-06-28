#!/usr/bin/env bash
# CLI escape hatch: submit a chapter pipeline (or stock WordCount) to Flink via the submitter
# container. The NestJS API runs the equivalent of this when you click "Run on Flink".
#
# Usage:
#   ./scripts/submit.sh wordcount          # stock Beam WordCount — validates the portability path
#   ./scripts/submit.sh ch01               # run chapter 1's pipeline
#   ./scripts/submit.sh ch09 -- --window=sliding   # pass extra args to the pipeline after `--`
set -euo pipefail
cd "$(dirname "$0")/.."

TARGET="${1:-}"
shift || true
# Anything after a literal `--` is forwarded to the pipeline.
EXTRA_ARGS=("$@")

if [ -z "$TARGET" ]; then
  echo "Usage: $0 <wordcount|chNN> [-- <extra pipeline args>]" >&2
  exit 1
fi

COMPOSE=(docker compose -f docker/docker-compose.yml)
RUN_ID="cli-$(date +%s)"

if [ "$TARGET" = "wordcount" ]; then
  echo ">>> Submitting stock apache_beam.examples.wordcount (job_name=wordcount-${RUN_ID})"
  "${COMPOSE[@]}" exec -T submitter python -m apache_beam.examples.wordcount \
    --runner=PortableRunner \
    --job_endpoint="${BEAM_JOB_ENDPOINT:-beam-job-server:8099}" \
    --artifact_endpoint="${BEAM_ARTIFACT_ENDPOINT:-beam-job-server:8098}" \
    --environment_type=EXTERNAL \
    --environment_config="${BEAM_ENVIRONMENT_CONFIG:-localhost:50000}" \
    --job_name="wordcount-${RUN_ID}" \
    --parallelism=2 \
    --input=/pipelines/_common/sample_text.txt \
    --output=/tmp/beam-artifact-staging/wordcount-out
  echo ">>> Done. Check the Flink UI: http://localhost:8081"
  exit 0
fi

# Map chNN -> the pipeline path (directories are chNN_<slug>/pipeline.py).
DIR=$(find beam-pipelines -maxdepth 1 -type d -name "${TARGET}_*" | head -n1)
if [ -z "$DIR" ]; then
  echo "ERROR: no pipeline directory matches '${TARGET}_*' under beam-pipelines/." >&2
  echo "Available:" >&2
  find beam-pipelines -maxdepth 1 -type d -name 'ch*' -exec basename {} \; | sort >&2
  exit 1
fi
REL="${DIR#beam-pipelines/}/pipeline.py"

echo ">>> Submitting ${REL} (job_name=${TARGET}-${RUN_ID})"
# ${arr[@]+"${arr[@]}"} expands safely to nothing when the array is empty, even under `set -u`
# on macOS's bash 3.2 (a bare "${EXTRA_ARGS[@]}" would error "unbound variable").
"${COMPOSE[@]}" exec -T submitter python "/pipelines/${REL}" \
  --run_id="${RUN_ID}" \
  ${EXTRA_ARGS[@]+"${EXTRA_ARGS[@]}"}
echo ">>> Done. Check the Flink UI: http://localhost:8081"

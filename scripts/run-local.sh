#!/usr/bin/env bash
# Start PostgreSQL, apply migrations, then run the API, the scheduler and N workers
# in the foreground. Ctrl-C stops everything gracefully.
#
#   scripts/run-local.sh            # 2 workers
#   WORKERS=4 scripts/run-local.sh
set -euo pipefail
cd "$(dirname "$0")/.."

WORKERS="${WORKERS:-2}"
CONCURRENCY="${CONCURRENCY:-4}"
LOG_DIR="${LOG_DIR:-.local-logs}"
mkdir -p "$LOG_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "created .env from .env.example"
fi
set -a; source .env; set +a
export FLOWFORGE_DEMO_LEDGER_PATH="${FLOWFORGE_DEMO_LEDGER_PATH:-$PWD/$LOG_DIR/ledger.jsonl}"

docker compose -f deployments/docker-compose.yml up -d --wait postgres
uv run flowforge migrate

pids=()
cleanup() {
  echo "stopping..."
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait
}
trap cleanup INT TERM

uv run flowforge api --port 8000 >"$LOG_DIR/api.log" 2>&1 & pids+=($!)
FLOWFORGE_METRICS_PORT=9101 uv run flowforge scheduler >"$LOG_DIR/scheduler.log" 2>&1 & pids+=($!)
for i in $(seq 1 "$WORKERS"); do
  FLOWFORGE_METRICS_PORT=$((9110 + i)) uv run flowforge worker --concurrency "$CONCURRENCY" \
    --worker-id "worker-$i" >"$LOG_DIR/worker-$i.log" 2>&1 & pids+=($!)
done

echo "API:       http://127.0.0.1:8000/docs"
echo "metrics:   :8000/metrics (api), :9101 (scheduler), :9111.. (workers)"
echo "logs:      $LOG_DIR/  (worker PIDs: ${pids[*]:2})"
wait

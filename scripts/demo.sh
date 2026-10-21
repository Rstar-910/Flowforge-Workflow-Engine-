#!/usr/bin/env bash
# End-to-end demo of retry and crash recovery against a running stack
# (start it first with scripts/run-local.sh in another terminal).
#
#  1. registers examples/demo-workflow.json  (A -> B,C -> D)
#  2. starts a run; B fails once and is retried
#  3. waits until C is running, then SIGKILLs the worker executing C
#  4. the scheduler recovers C after the lease expires; another worker reruns it
#  5. prints the attempt history
#
# This demonstrates the tested behaviour only; it does not prove every failure is safe.
set -euo pipefail
cd "$(dirname "$0")/.."
API="${API:-http://127.0.0.1:8000}"
KEY="${KEY:-dev-user-key}"
H=(-H "X-API-Key: $KEY" -H "Content-Type: application/json")

def_id=$(curl -sf "${H[@]}" -d @examples/demo-workflow.json "$API/v1/workflow-definitions" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
run_id=$(curl -sf "${H[@]}" -H "Idempotency-Key: demo-$(date +%s)" -d '{"input": {"rows": 8}}' \
  "$API/v1/workflow-definitions/$def_id/runs" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
echo "run: $run_id"

echo "waiting for task C to start..."
owner=""
for _ in $(seq 1 120); do
  owner=$(curl -sf "${H[@]}" "$API/v1/runs/$run_id/tasks" | python3 -c '
import json,sys
for t in json.load(sys.stdin)["items"]:
    if t["task_key"] == "C" and t["status"] == "RUNNING":
        print(t["lease_owner"])')
  [[ -n "$owner" ]] && break
  sleep 0.5
done
[[ -n "$owner" ]] || { echo "C never started"; exit 1; }
echo "C is running on $owner"
scripts/inject-worker-crash.sh "$owner"

echo "waiting for the run to finish (lease expiry + rerun of a 20s task)..."
for _ in $(seq 1 240); do
  status=$(curl -sf "${H[@]}" "$API/v1/runs/$run_id" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')
  [[ "$status" =~ ^(SUCCEEDED|FAILED|CANCELLED)$ ]] && break
  sleep 0.5
done
echo "run status: $status"
uv run flowforge inspect "$run_id"

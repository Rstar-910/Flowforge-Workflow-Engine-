#!/usr/bin/env bash
# SIGKILL a local worker process by its worker id (as shown in tasks.lease_owner).
#
#   scripts/inject-worker-crash.sh worker-1
#
# Other failure-injection options:
#   FLOWFORGE_ENABLE_FAULT_INJECTION=true FLOWFORGE_FAULT_POINTS=crash_after_claim  uv run flowforge worker
#   FLOWFORGE_ENABLE_FAULT_INJECTION=true FLOWFORGE_FAULT_POINTS=crash_after_handler uv run flowforge worker
#   FLOWFORGE_ENABLE_FAULT_INJECTION=true FLOWFORGE_FAULT_POINTS=skip_heartbeats    uv run flowforge worker
#   kill -STOP <pid>; sleep 20; kill -CONT <pid>      # pause a worker past its lease
set -euo pipefail
worker_id="${1:?usage: $0 <worker-id>}"
# Match every process for this worker id: when started via `uv run`, both the uv
# wrapper and the Python worker match, and killing only the wrapper would leave the
# worker running.
pids=$(pgrep -f -- "flowforge worker .*--worker-id ${worker_id}( |\$)" || true)
if [[ -z "$pids" ]]; then
  echo "no local process found for worker id ${worker_id}" >&2
  exit 1
fi
echo "SIGKILL worker ${worker_id} (pids: $(echo $pids))"
kill -KILL $pids

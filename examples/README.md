# Sample workflows

Each file is a workflow definition you can register and run against a local stack
(`make run`). All of them use only the built-in handlers in
[`src/flowforge/handlers/builtin.py`](../src/flowforge/handlers/builtin.py).

| file | shape | what it shows | expected result |
|---|---|---|---|
| [`simple-workflow.json`](simple-workflow.json) | `fetch → total → report` | the smallest useful run; outputs flow downstream | SUCCEEDED, output `{"report": {"report": {"total": 970}}}` for `{"rows": 20}` |
| [`report-workflow.json`](report-workflow.json) | `fetch → (sum, max, mean) → publish` | fan-out and fan-in; per-task timeouts and retry policies | SUCCEEDED, output `{"publish": {"report": {"sum": 970, "max": 97, "mean": 48.5}}}` |
| [`demo-workflow.json`](demo-workflow.json) | `A → (B, C) → D` | B fails once and is retried; C sleeps 20 s so its worker can be killed. Used by `make demo` | SUCCEEDED; B has 2 attempts; C has 2 if its worker was killed (`LEASE_EXPIRED`, then `SUCCEEDED`) |
| [`failure-workflow.json`](failure-workflow.json) | `fetch → validate → transform → load`, plus `fetch → audit` | a permanent failure: dependents become `UPSTREAM_FAILED`, the independent branch still finishes | FAILED; `validate` FAILED, `transform` and `load` UPSTREAM_FAILED, `fetch` and `audit` SUCCEEDED |
| [`deadline-workflow.json`](deadline-workflow.json) | `fetch → total → export` | a 120 s workflow deadline and a 30 s queue timeout on `export` | with workers running: SUCCEEDED in well under a second. With **no workers** running (API and scheduler only): about 120 s after creation the scheduler fails the run with `failure_reason: "workflow deadline exceeded"` and all three tasks become CANCELLED |
| [`side-effect-workflow.json`](side-effect-workflow.json) | `charge` | a simulated external side effect (a ledger file) de-duplicated on the engine's operation key | SUCCEEDED, output `{"charge": {"applied": true}}`; one ledger line per run |

## Running one

With the stack running (`make run`, or the Compose `app` profile):

```bash
H='X-API-Key: dev-user-key'          # local development key from .env.example
DEF=$(curl -s -H "$H" -H 'Content-Type: application/json' \
  -d @examples/failure-workflow.json http://127.0.0.1:8000/v1/workflow-definitions | jq -r .id)
RUN=$(curl -s -H "$H" -H 'Content-Type: application/json' \
  -d '{"input": {"rows": 20}}' http://127.0.0.1:8000/v1/workflow-definitions/$DEF/runs | jq -r .id)
curl -s -H "$H" http://127.0.0.1:8000/v1/runs/$RUN | jq '{status, task_counts, output}'
```

Or use the dashboard: **New run → Register definition**, paste the file, start it, and
watch the graph at http://127.0.0.1:8000/ui/.

`uv run flowforge inspect $RUN` prints every task's attempts straight from the database.

## The side-effect example

`record_effect` appends to the file named by `FLOWFORGE_DEMO_LEDGER_PATH`, standing in for
an external system such as a payment provider. `.env.example` points it at
`.local-logs/ledger.jsonl`; in Compose the workers share a volume at
`/var/lib/flowforge/ledger.jsonl`. With `"use_operation_key": true` the "external
system" ignores a second write with the same operation key, which is how a handler
should make an at-least-once engine safe for side effects. The test
`test_crash_after_side_effect…` in
[`tests/failure/test_process_failures.py`](../tests/failure/test_process_failures.py)
shows both cases: without the key a crash after the effect duplicates it, with the key
it does not.

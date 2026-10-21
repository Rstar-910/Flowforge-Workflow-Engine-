# Failure model: guarantees and limitations

This document describes the behaviour that is implemented and tested. Each scenario
names the test that demonstrates it. Simulated failures show the tested behaviour
only. They do not prove every possible distributed failure is handled safely.

## Guarantees

1. **Durability of accepted work.** A run is acknowledged (201/200) only after one
   transaction has committed the run, all its tasks, its dependency edges, and its
   idempotency record. Pending work lives only in PostgreSQL, and no component keeps
   it in memory. If any process restarts, the work is still there.
2. **Dependency order.** A task becomes READY only when every dependency is SUCCEEDED.
   The decision is made under the run lock (ADR 0005).
   Tests: `test_diamond_dependencies_and_run_output`,
   `test_random_dag_respects_dependencies` (8 seeded random DAGs with flaky tasks),
   `test_fan_in_promotion_has_no_write_skew`.
3. **Exclusive leases.** A task attempt is held by at most one worker. Claims use
   `FOR UPDATE SKIP LOCKED`, and every claim creates a new attempt id. Test:
   `test_concurrent_claimers_never_share_a_task` (30 claimers, 300 tasks, 3 trials;
   verified against persisted attempts).
4. **Stale results are never accepted.** Lease renewal and result commits are
   conditional on the attempt id (the fencing token). A worker whose lease was
   recovered cannot overwrite the task, the attempt history, or the run. Its report is
   kept as a `stale_result_rejected` event.
   Tests: `test_stale_worker_cannot_overwrite_newer_attempt`,
   `test_paused_worker_loses_lease_and_its_late_result_is_rejected` (SIGSTOP),
   `test_worker_without_heartbeats_cannot_commit_after_takeover`,
   `test_long_database_outage_leads_to_safe_takeover`.
5. **At most one accepted success per task.** Benchmarks also count duplicate accepted
   results; the count is 0 in all recorded runs.
6. **Bounded retries.** Every attempt counts against `attempt_limit`, including
   attempts that end in lease expiry. Only an admin's manual retry raises the limit.
   Tests: `test_retries_are_bounded_then_failure_propagates`,
   `test_lease_expiry_consumes_attempts_and_terminates`.
7. **Idempotent submission.** Within a tenant, one key maps to exactly one run, even
   under concurrent submissions. Reusing a key with a different body gets 409.
   Tests: `test_concurrent_idempotent_submissions_create_one_run` (25 concurrent),
   `test_idempotent_submission_via_api`.
8. **Invalid graphs never execute.** Cycles, unknown references, unknown handlers, and
   malformed fields are rejected before anything is stored. Tests:
   `test_invalid_graphs_are_rejected_before_storage` plus Hypothesis property tests.

## Execution semantics: at-least-once

**FlowForge does not provide exactly-once execution.** A handler may run more than once
for the same task:

- after a worker crash or pause,
- after a lease expiry,
- after a timeout (a thread handler may even still be running),
- after a retryable failure,
- after a manual retry.

What the engine guarantees is the list above: **at most one accepted result per
attempt, and at most one success per task**.

### The ambiguous side effect

Consider this sequence: a handler charges a card, the charge succeeds, and the worker
is killed before it commits SUCCEEDED. The engine sees only that the lease expired. It
records the attempt as `LEASE_EXPIRED`. The `lease_expired` event carries
`"outcome_known": false`. The engine then schedules another attempt. It cannot know
whether the card was charged.

`test_crash_after_side_effect_before_commit` reproduces this with a real process that
crashes at the `crash_after_handler` fault point:

| handler mode | ledger entries after recovery | outcome |
|---|---|---|
| no downstream de-duplication (`use_operation_key=false`) | **2** | duplicate effect: the documented at-least-once behaviour |
| de-duplicates on `ctx.operation_key` | **1** | the second attempt detects the earlier effect and returns `duplicate_of_existing` |

**What handlers must do:**

- Pass `ctx.operation_key` (`<run_id>/<task_key>`, stable across attempts) as the
  idempotency key to downstream APIs, such as payment providers or email services.
- Where a downstream system supports fencing, pass `ctx.fencing_token` (the attempt
  number, which only increases). The downstream system can then reject writes from an
  older attempt.
- Make database writes idempotent (upserts keyed by the operation key).
- Register handlers that cannot be made idempotent with `idempotent=False`. Then decide
  deliberately whether they should retry at all (`max_attempts: 1`) and treat manual
  retries with care.

### Leases do not stop old workers

A lease is a failure *detector*, not a kill switch:

- A worker that is paused by GC, SIGSTOP, a VM freeze, or a network partition keeps
  executing after its lease is reassigned. When it can, the old worker learns that its
  lease is lost: either renewal fails or its local deadline passes. It then signals its
  handler to stop. That is best effort. Thread handlers stop only if they cooperate.
- The fencing token protects the engine's state completely. It protects external
  systems only if they check the token or de-duplicate on the operation key.

## Scenario behaviour

| scenario | behaviour | test |
|---|---|---|
| Worker crashes right after claiming | Task stays RUNNING until the lease expires (default 15 s). The scheduler marks attempt 1 `LEASE_EXPIRED` and applies the retry policy. Another worker runs attempt 2. | `test_worker_crash_immediately_after_claim` (injected `os._exit`) |
| Worker SIGKILLed while executing | Same. Measured recovery time is in the benchmark report. | `test_worker_sigkill_during_execution_is_recovered` |
| Worker crashes after the side effect, before commit | Side effect done, result lost, task re-executed (see above). | `test_crash_after_side_effect_before_commit` |
| Worker paused (SIGSTOP), then resumed | Lease recovered, successor completes, resumed worker's result rejected. | `test_paused_worker_loses_lease_and_its_late_result_is_rejected` |
| Heartbeats delayed or absent | Same as a pause; the worker is still executing. | `test_worker_without_heartbeats_cannot_commit_after_takeover` |
| Graceful shutdown (SIGTERM) | The worker stops claiming and drains in-flight tasks within the grace period (default 30 s). After that it signals cancellation and records a retryable `WorkerShutdown` failure. A second signal exits immediately. | `test_graceful_shutdown_finishes_inflight_and_stops_claiming` |
| Scheduler crash or restart | No state is lost. Retry timing (`next_attempt_at`) and lease expiry are stored in rows. While the scheduler is down, due retries wait and expired leases are not recovered. Ordinary execution and dependency promotion continue, because workers do them inside completion transactions. | `test_scheduler_sigkill_and_restart_with_retry_wait`, `test_scheduler_restart_with_retry_wait_tasks` |
| Several schedulers | Safe: SKIP LOCKED plus lock-and-recheck. They duplicate polling work only. | by construction; not load-tested |
| API restart | Stateless. In-flight requests either committed or not. Clients retry with the same Idempotency-Key. | `test_idempotent_submission_via_api` |
| Short database outage (shorter than the lease) | Workers back off on claim errors, and renewals fail and are retried. Commits retry with backoff. The task completes in one attempt. | `test_short_database_outage_is_survived` (TCP proxy cut for 1.5 s) |
| Long database outage for one worker (longer than the lease) | The worker stops its handler once its local deadline passes. The scheduler recovers the lease and another worker completes the task. On reconnect, the partitioned worker's commit is rejected. | `test_long_database_outage_leads_to_safe_takeover` |
| Database outage for the API | `/health/ready` returns 503. Requests get 503 `unavailable` with `Retry-After`, and no internals are exposed. | `test_database_outage_returns_503_without_internals` |
| Commit acknowledgement lost (committed, then the connection dropped) | The worker retries the commit. It finds its own attempt already finished and treats that as `already_recorded`, not as stale. | code path in `complete_attempt`; not fault-injected |
| Retry exhaustion | Task FAILED (the dead-letter state), with full attempt history, errors, and timestamps kept. Dependents become UPSTREAM_FAILED, and the run becomes FAILED once independent branches finish. An admin can retry it manually. | `test_retries_are_bounded_then_failure_propagates`, `test_manual_retry_reopens_failed_task_and_dependents` |
| Handler timeout | `async`: interrupted. `process`: killed. `thread`: abandoned and counted as a zombie against capacity (ADR 0008). The attempt is `TIMED_OUT` and retried if the policy allows. | `test_execution_timeout_async_and_process`, `test_thread_timeout_counts_zombie_until_thread_returns` |
| Cancellation during execution | See `state-machines.md`. Cooperative handlers end CANCELLED. Uncooperative ones record their true outcome, and the run still ends CANCELLED. | `test_cancel_run_with_ready_blocked_and_running_tasks`, `test_cancel_with_uncooperative_handler_waits_for_completion` |
| Duplicate submissions | Same key and same body return the existing run. Same key with a different body gets 409. | concurrency and API tests |
| Bug or manual database edit leaves a run inconsistent | The scheduler reconciler re-applies progression. | `test_reconciler_repairs_progression_after_manual_db_edit` |

## Known limitations and open risks

- **Clock assumptions.** Expiry uses only the database clock. A worker's local deadline
  relies on its monotonic clock running no faster than real time. That is normally
  true, but extreme drift could make a worker stop work late. Correctness does not
  depend on it, only wasted effort does.
- **Recovery latency.** Expected recovery is between the lease duration and lease +
  scheduler interval + claim poll. With the defaults (15 s lease, 1 s interval) that is
  roughly 15 to 17 s. Shorter leases recover faster but cost more heartbeat writes and
  raise the risk of false expiry during GC pauses or database hiccups.
- **Per-run completion serialisation.** The run lock (ADR 0005) serialises completions
  within one run. Very wide single runs (thousands of parallel tasks) are limited by
  this. Different runs do not contend.
- **Thread handlers** can outlive their timeout and keep consuming a slot until they
  return. A handler stuck forever permanently reduces that worker's capacity, and the
  worker should be restarted. Use `process` isolation for untrusted-duration blocking
  work.
- **Lease expiry while the database is down for everyone.** If the scheduler also cannot
  reach PostgreSQL, nothing is recovered until the database returns. Workers that can
  reconnect then commit normally, provided their attempt is still current.
- **Events table growth.** `task_events` is append-only and never pruned. A retention
  job or partitioning is future work, to be added when volume justifies it.
- **Queue timeout and workflow deadline** (spec section 11) are not implemented. Only
  execution timeouts and lease timeouts exist.
- **Compensation / sagas** are not implemented.
- **Rate limiting** is per API process, not global.
- **The ambiguous commit path** (committed, acknowledgement lost) is handled in code but
  has no fault-injection test.
- The test suite runs PostgreSQL in Docker on one host. It does not exercise real
  network partitions between hosts, disk failures, or PostgreSQL failover. Under
  synchronous replication the transactional guarantees should carry over to failover;
  under asynchronous replication, recently committed results can be lost. Neither is
  tested.

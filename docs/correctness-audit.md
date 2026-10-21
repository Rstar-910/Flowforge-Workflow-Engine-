# Correctness and recovery audit

This audit covers every operation that changes task or run state, the lock and guard
each one relies on, the operations that can hit the same rows at the same moment, and
the test that exercises each pairing. Findings and fixes are listed at the end.

Scope: a single PostgreSQL primary, with any number of API, scheduler and worker
processes on one host. **Multi-host network partitions and PostgreSQL failover are not
tested** (see "Not covered").

## 1. Every state-changing operation

The engine uses one lock order everywhere: the transaction's *own RUNNING task row*,
then the *run row* (`FOR NO KEY UPDATE`), then *non-running task rows*. Operations that
start from the run row never write RUNNING rows. Claims and promotions use only
`SKIP LOCKED`. ADR 0005 explains why this order cannot deadlock.

| # | Operation | Code | Locks | Guard re-checked after locking |
|---|---|---|---|---|
| 1 | Claim (one autocommit statement) | `execution.claim_tasks` | READY rows and PENDING run, both `SKIP LOCKED` (never waits) | `status = 'READY'` (EvalPlanQual) |
| 2 | Renew lease | `execution.renew_leases` | own RUNNING rows | `RUNNING` + `current_attempt_id` + `lease_owner` |
| 3 | Complete attempt | `execution.complete_attempt` → `finish_attempt` → `settle_run` | own row, then run, then children | `RUNNING` + `current_attempt_id`; cancellation re-read under the run lock |
| 4 | Recover expired lease | `recovery._recover_one` | expired row, then run | `RUNNING` + `current_attempt_id` + `lease_expires_at < now` *at write time* |
| 5 | Promote due retry | `recovery.promote_due_retries` | RETRY_WAIT rows, `SKIP LOCKED` | `RETRY_WAIT` + due |
| 6 | Cancel run | `control.cancel_run` → `begin_stopping` | run, then non-running rows | run status under the lock |
| 7 | Deadline stop | `timeouts._stop_overdue_run` → `begin_stopping` | run, then non-running rows | run `PENDING`/`RUNNING` and overdue, under the lock |
| 8 | Queue timeout | `timeouts._fail_queued_task` | run, then the READY row | `READY` + still expired |
| 9 | Manual retry | `control.retry_failed_task` | run, then FAILED and UPSTREAM_FAILED rows | task `FAILED`; run `RUNNING`/`FAILED`; deadline not passed |
| 10 | Reconcile | `recovery.reconcile_runs` | run | run status |
| 11 | Retention | `retention.*` | run, `SKIP LOCKED` | run finished and older than the cutoff |

## 2. Race matrix: two valid operations at once

Each race was tested in one of two ways:

- **Forced:** an external transaction holds a row lock until the operations are staged
  in a known order. The tests track the exact backend PIDs queued, rather than counting
  waiters, and refuse to start if a stray waiter already exists. Which row gets held
  matters:
  - When both operations take the run lock as their *first* lock (deadline vs cancel),
    holding the run row and releasing it grants them in arrival order.
  - When one operation is a completion or a lease recovery, staging holds the **task**
    row instead. Its fenced UPDATE then blocks before it holds any lock on the run.
    Otherwise its earlier event insert has already taken `FOR KEY SHARE` on the run
    through the foreign key, and PostgreSQL lets a transaction that already holds a lock
    on a row skip the wait queue when it upgrades. Run-row staging was therefore
    nondeterministic for these races: about 1 run in 3 took the other order. The outcome
    was still consistent, but the test could not claim to force the order.
- **Randomized:** both operations are launched together many times with random
  offsets. The splits below show how often each order won across 3 consecutive runs of
  `tests/integration/test_races.py`.

| Race | Method | Result | Test |
|---|---|---|---|
| Completion vs lease recovery (same attempt) | randomized × 30 | exactly one wins every time; completion won 23–27 / recovery 3–7 | `test_completion_vs_lease_recovery_exactly_one_wins` |
| Renewal vs lease recovery | randomized × 30 | never both; renewed 17–27 / recovered 3–13 | `test_renewal_vs_lease_recovery_never_both` |
| Cancel commits while a failing completion (already decided 'retry') is blocked | forced (task-row staging) | the pending retry becomes CANCELLED; nothing is claimable | `test_cancel_commits_before_a_failing_completion_is_applied` |
| Cancel commits while a lease recovery is blocked | forced (task-row staging) | recovered task becomes CANCELLED | `test_cancel_commits_before_a_lease_recovery_is_applied` |
| Retry promotion vs cancel | randomized × 25 | always CANCELLED; nothing claimable | `test_retry_promotion_vs_cancel_never_leaves_claimable_work` |
| Claim vs cancel | randomized × 15 | claimed 8 or 0; the run always drains to CANCELLED | `test_claim_vs_cancel_then_drain` |
| Manual retry vs cancel | randomized × 20 | serialized: cancel-first 10–13 / retry-first 7–10, both consistent | `test_manual_retry_vs_cancel_are_serialised` |
| Last completion vs deadline | forced deadline-first (task-row staging), sequential completion-first, randomized × 15 | FAILED by deadline with the task SUCCEEDED, or SUCCEEDED; never a mix | `test_last_completion_vs_deadline_both_orders_are_consistent` |
| Deadline vs user cancel | randomized × 20, plus forced deadline-first | CANCELLED (no deadline mark) or FAILED (deadline mark) | `test_deadline_vs_user_cancel_one_consistent_outcome` |
| Queue timeout vs claims (6 claimers, 40 expired tasks) | randomized | every task claimed **or** timed out, never both or neither | `test_queue_timeout_races_with_claims_safely` |
| Two schedulers, same moment | concurrent | 8 recoveries, 8 promotions, 5 deadlines and 5 queue timeouts, each applied **once** | `test_two_schedulers_apply_each_effect_exactly_once` |
| Sibling completions (fan-in of 20) | concurrent | the child is promoted exactly once | `test_fan_in_promotion_has_no_write_skew` |
| Retention vs a transaction holding the run | forced | retention skips the run | `test_runs_locked_by_another_transaction_are_skipped` |

### Stale workers

| Scenario | Result | Test |
|---|---|---|
| Lease recovered and re-claimed; old worker reports | rejected and recorded as `stale_result_rejected` | `test_stale_worker_cannot_overwrite_newer_attempt` |
| Restarted worker **reuses its worker id**; old process reports | rejected: fencing is by attempt id, not worker id | `test_stale_worker_reusing_its_worker_id_cannot_touch_the_new_attempt` |
| Attempt 1 expires, task FAILED, **manual retry**, attempt 2 running; attempt 1 reports | rejected; attempt 2 unaffected | `test_stale_attempt_cannot_overwrite_a_manually_retried_task` |
| Worker paused (SIGSTOP) or heartbeats stop | rejected after takeover | `test_paused_worker_…`, `test_worker_without_heartbeats_…` |
| Worker partitioned, **scheduler SIGKILLed and replaced**, new scheduler recovers, worker B finishes, partition heals | A's late commit rejected; output is B's | `test_stale_worker_after_scheduler_restart_and_reconnect` |
| Worker partitioned longer than its lease | rejected after reconnect | `test_long_database_outage_leads_to_safe_takeover` |
| COMMIT succeeded, acknowledgement lost, worker retries | recognized as `already_recorded`, written once, no false stale event | `test_lost_commit_acknowledgement_is_not_mistaken_for_a_stale_result` |
| **PostgreSQL kills worker sessions** every ~20 ms during a 62-task run (183–206 kills, 11–16 interrupted claims, commits and renewals) | run SUCCEEDED; exactly one accepted success per task | `test_server_side_session_kills_during_a_workflow` |

## 3. Global invariants

The `db` test fixture checks `tests/support/invariants.py` against PostgreSQL after
**every** integration and failure test. A test opts out only with the
`no_invariants` marker; one schema test does, because it probes constraints by
inserting a bare attempt. The invariants are:

- no READY, RETRY_WAIT or BLOCKED task in a stopping or finished run;
- no RUNNING task in a finished run;
- every RUNNING task has exactly one RUNNING attempt, and it equals the task's
  `current_attempt_id` and `attempt_count`;
- no open attempt on a task that isn't RUNNING it;
- attempt numbers are dense, 1..`attempt_count`;
- at most one SUCCEEDED attempt per task;
- a SUCCEEDED task's last attempt succeeded;
- a terminal run has no active tasks;
- a SUCCEEDED run has only SUCCEEDED tasks;
- every READY task has a `ready_at`;
- no task is READY, RUNNING or SUCCEEDED while a dependency hasn't succeeded;
- a deadline-stopped run ends FAILED.

`test_invariant_checker_detects_corruption` proves the checker reports deliberate
corruption. All 174 tests pass with the invariants enforced.

## 4. Findings and fixes

1. **Manual retry after the deadline had passed.** A run that failed for another reason
   before its deadline could be re-opened after the deadline. The deadline sweep would
   then stop it again within a second, cancelling the retried task. *Fix:* retry is
   refused once `deadline_at` has passed.
   *Test:* `test_manual_retry_refused_once_the_deadline_has_passed`, which failed
   before the fix.
2. **A worker stalled on connections the server had closed while idle.** Workers run
   without pool pre-ping (ADR 0011), so every pooled connection killed while idle
   (server restart, idle timeout, `pg_terminate_backend`) cost a failed operation plus
   exponential backoff. Under repeated kills a workflow made **no progress in 45 s**.
   *Fix (ADR 0015):* when SQLAlchemy flags the connection as invalidated, the operation
   is retried once immediately. The same workload then completed in about 3 s.
   *Tests:* `test_reconnect.py` and `test_server_side_session_kills_during_a_workflow`.
   Disabling the retry makes the latter time out again.
3. **Test weaknesses fixed along the way.** None of these were engine bugs.
   - The first session-kill test passed without any kill hitting live work. It now
     requires evidence that operations were interrupted.
   - The lost-acknowledgement test relied on a log line that the reconnect fix made
     disappear, so it could have passed without a retry ever happening. The worker now
     emits an explicit `commit_already_recorded` event, and the test requires it.
   - Repeated trials in the race tests could claim a previous trial's task. Each trial
     now starts from an empty database.
4. **A PostgreSQL behavior worth knowing: lock-upgrade queue jumping.** A completion
   that has already inserted an event holds `FOR KEY SHARE` on the run through the
   foreign key. Its later request for the run lock is an upgrade, and PostgreSQL lets it
   bypass transactions already queued for that row. Correctness is unaffected: every
   outcome observed was one of the consistent ones, and the invariants hold. But it made
   three "forced" tests nondeterministic. One of them failed in a full-suite run (1 in 3
   runs in a loop). They now stage on the task row and passed 10 out of 10 consecutive
   runs.

No issue was found with ownership fencing, lease recovery, cancellation, retry
promotion, or run completion under the races above.

## 5. Not covered

- Multi-host deployments: real network partitions between hosts, and clock skew
  between worker hosts. Lease expiry uses only the database clock, so skew affects
  only a worker's own deadline for when to stop working.
- PostgreSQL failover or replica promotion, and loss of recently committed
  transactions under asynchronous replication.
- A lost acknowledgement on a *claim* rather than a commit. The worker would not know
  it owns the claimed tasks; they would be recovered after one lease period, using up
  one attempt. This is reasoned about, not fault-injected.
- Byzantine failures (a worker that forges attempt ids) and disk-level corruption.

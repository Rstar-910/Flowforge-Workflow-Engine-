# ADR 0013: Opt-in, batched retention run by the scheduler

- Status: accepted

## Context

Several tables grow without bound:

- **`task_events`** is the highest-volume table, at about 5–6 rows per task.
- **Finished runs**, together with their tasks, dependencies and attempts.
- **The `workers` registry**, which gains a row every time a worker starts.

Unbounded growth costs disk space and slows any broad scan. ADR 0012 also showed that
dead tuples on the hot tables degrade plans that depend on statistics.

## Decision

1. **Two independent, opt-in policies.** Deleting history is a product decision, so
   neither policy is on by default.
   - `FLOWFORGE_EVENT_RETENTION_DAYS` deletes the events of finished runs older than N
     days. It keeps runs, tasks and attempts, which are the history that matters most
     for debugging, and sets `workflow_runs.events_pruned_at`. The API and dashboard
     show that marker, so empty event lists are explained.
   - `FLOWFORGE_RUN_RETENTION_DAYS` deletes finished runs older than N days.
     `ON DELETE CASCADE` removes their tasks, dependencies, attempts, events and
     idempotency records in the same statement. Definitions are never deleted.
2. **Worker registry cleanup is on by default** (`FLOWFORGE_WORKER_RETENTION_HOURS`,
   default 168). These are infrastructure rows, not user data. A live worker refreshes
   its row on every heartbeat, so only workers that are really gone age out.
3. **The scheduler applies retention every `retention_interval_seconds` (default 60).**
   Each statement handles at most `retention_batch_size` runs (default 100), and at
   most 20 batches run per cycle. A large backlog is therefore worked off across cycles
   instead of in one long, lock-heavy transaction.
4. **Safety rules:**
   - Only runs that are SUCCEEDED, FAILED or CANCELLED are eligible, judged by
     `completed_at`. Active runs are never touched, however old they are.
   - Run rows are taken with `FOR NO KEY UPDATE SKIP LOCKED`, the engine's run-lock mode
     (ADR 0005). Retention never waits for, or races with, a transaction holding the
     run, such as an admin's manual retry of a FAILED run.
   - A manual retry clears `events_pruned_at`, so the re-opened run's new events get
     normal retention later.
   - A stale worker that reports on a run that has since been deleted gets its report
     logged and dropped, instead of hitting a foreign-key error on every retry.

## Consequences

- **The idempotency window equals the run retention period.** After a run is deleted,
  its idempotency key can create a new run. Clients that retry for longer than that
  need their own de-duplication.
- Deleting events is `DELETE`, not partition dropping. If retention deletes ever become
  a measured cost, the next step is to partition `task_events` by time.
- Tests (`tests/integration/test_retention.py`) cover:
  - opt-in defaults;
  - event pruning that keeps runs, tasks and attempts;
  - the full cascade, plus reuse of the idempotency key after deletion;
  - active runs being immune;
  - lock skipping;
  - bounded batches;
  - clearing the marker on manual retry;
  - worker cleanup;
  - the scheduler interval.

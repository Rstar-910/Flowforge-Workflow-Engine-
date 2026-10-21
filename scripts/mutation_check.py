"""Mutation check: prove each failure-model test fails when its protection is removed.

For every protection the failure model relies on, this script edits the source to
remove it, runs the tests that are supposed to catch that, and restores the file.

    uv run python scripts/mutation_check.py            # all mutations
    uv run python scripts/mutation_check.py M01 M07     # selected ones
    uv run python scripts/mutation_check.py --verify    # only check that every edit applies

A mutation is **killed** when at least one of its tests fails, which is what we want.
It **survived** when they all pass: either a test is missing, or the removed code is
redundant (an *equivalent mutant*; such cases are listed with the reason in
EXPECTED_SURVIVORS and documented).

Each edit must match its file exactly once, so a refactor cannot silently turn a
mutation into a no-op. Files are always restored, including on errors or Ctrl-C, and
the restore is verified. Results go to stdout and to
docs/mutation-results.md / benchmarks/results/mutation-results.json.

Needs the docker-compose PostgreSQL (tests use the flowforge_test database).
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src" / "flowforge"
PY = sys.executable
TEST_TIMEOUT = 45  # seconds per test while mutated: a hang (lost liveness) counts as killed


@dataclass
class Mutation:
    id: str
    protection: str
    scenario: str
    file: str  # relative to src/flowforge
    edits: list[tuple[str, str]]
    tests: list[str]
    notes: str = ""
    result: dict[str, object] = field(default_factory=dict)


T_REL = "tests/integration/test_reliability.py"
T_RACE = "tests/integration/test_races.py"
T_CONC = "tests/integration/test_concurrency.py"
T_EXEC = "tests/integration/test_execution.py"
T_DEAD = "tests/integration/test_deadlines.py"
T_RET = "tests/integration/test_retention.py"
T_API = "tests/integration/test_api.py"
T_PROC = "tests/failure/test_process_failures.py"

MUTATIONS: list[Mutation] = [
    Mutation(
        "M01",
        "completion is fenced on the attempt id (lease token)",
        "stale worker / paused worker / partitioned worker cannot overwrite a newer attempt",
        "engine/execution.py",
        [("           AND current_attempt_id = :attempt_id\n", "")],
        [
            f"{T_REL}::test_stale_worker_cannot_overwrite_newer_attempt",
            f"{T_RACE}::test_stale_worker_reusing_its_worker_id_cannot_touch_the_new_attempt",
            f"{T_RACE}::test_stale_attempt_cannot_overwrite_a_manually_retried_task",
        ],
    ),
    Mutation(
        "M02",
        "lease recovery re-checks expiry at write time",
        "a worker that renewed in time is not robbed of its lease",
        "engine/execution.py",
        [
            (
                "           AND (NOT CAST(:require_expired AS boolean)\n"
                "                OR lease_expires_at < statement_timestamp())\n",
                "",
            )
        ],
        [f"{T_RACE}::test_renewal_vs_lease_recovery_never_both"],
    ),
    Mutation(
        "M03",
        "lease renewal is restricted to the lease owner",
        "another worker cannot extend (and so keep alive) a lease it does not own",
        "engine/execution.py",
        [("       AND t.lease_owner = :worker_id\n", "")],
        [f"{T_REL}::test_renewal_keeps_lease_and_recovery_ignores_live_leases"],
    ),
    Mutation(
        "M04",
        "run-row lock serialises progression",
        "sibling completions cannot both miss promoting their shared child (write skew)",
        "engine/sql.py",
        [(".with_for_update(key_share=True)", "")],
        [f"{T_CONC}::test_fan_in_promotion_has_no_write_skew"],
    ),
    Mutation(
        "M05",
        "a pending retry is cancelled if the run was cancelled meanwhile",
        "cancellation during execution cannot leave claimable work behind",
        "engine/execution.py",
        [
            (
                '    if run["status"] == RunStatus.CANCELLING and task_status in (',
                '    if False and run["status"] == RunStatus.CANCELLING and task_status in (',
            )
        ],
        [
            f"{T_RACE}::test_cancel_commits_before_a_failing_completion_is_applied",
            f"{T_RACE}::test_cancel_commits_before_a_lease_recovery_is_applied",
        ],
    ),
    Mutation(
        "M06",
        "claims lock READY rows (FOR UPDATE SKIP LOCKED)",
        "two workers never own the same attempt",
        "engine/execution.py",
        [
            (
                "         LIMIT :limit\n           FOR UPDATE SKIP LOCKED\n    ), claimed AS (",
                "         LIMIT :limit\n    ), claimed AS (",
            )
        ],
        [f"{T_CONC}::test_concurrent_claimers_never_share_a_task"],
    ),
    Mutation(
        "M07",
        "retries (and lease expiries) are bounded by attempt_limit",
        "retry exhaustion terminates; a poison task cannot loop forever",
        "engine/execution.py",
        [("if f.retryable and attempt_number < attempt_limit:", "if f.retryable:")],
        [
            f"{T_REL}::test_lease_expiry_consumes_attempts_and_terminates",
            f"{T_EXEC}::test_retries_are_bounded_then_failure_propagates",
        ],
    ),
    Mutation(
        "M08",
        "a retried commit recognises its own already-recorded result",
        "commit acknowledgement lost",
        "engine/execution.py",
        [
            (
                "                if prior in _OWNER_OUTCOMES:",
                "                if False and prior in _OWNER_OUTCOMES:",
            )
        ],
        [f"{T_PROC}::test_lost_commit_acknowledgement_is_not_mistaken_for_a_stale_result"],
    ),
    Mutation(
        "M09",
        "one immediate retry when a pooled connection was already dead",
        "PostgreSQL terminates worker sessions (restart, idle timeout, kill)",
        "db/session.py",
        [
            (
                "        if not exc.connection_invalidated:\n            raise",
                "        if True:\n            raise",
            )
        ],
        [
            "tests/integration/test_reconnect.py",
            f"{T_PROC}::test_server_side_session_kills_during_a_workflow",
        ],
    ),
    Mutation(
        "M10",
        "idempotency key reserved in the submission transaction",
        "duplicate submissions (also concurrent) create one run",
        "engine/submission.py",
        [("        if idempotency_key is not None:", "        if False:")],
        [f"{T_CONC}::test_concurrent_idempotent_submissions_create_one_run"],
    ),
    Mutation(
        "M11",
        "failure propagates to dependents (UPSTREAM_FAILED)",
        "retry exhaustion / permanent failure fails the run",
        "engine/progression.py",
        [('        await propagate_upstream_failures(conn, run["id"])', "        pass")],
        [f"{T_EXEC}::test_retries_are_bounded_then_failure_propagates"],
    ),
    Mutation(
        "M12",
        "the scheduler stops overdue runs",
        "workflow deadline passes",
        "engine/timeouts.py",
        [("        if await _stop_overdue_run(engine, run_id):", "        if False:")],
        [
            f"{T_DEAD}::test_deadline_with_nothing_running_fails_immediately",
            f"{T_DEAD}::test_deadline_stops_a_running_workflow_and_fails_it",
        ],
    ),
    Mutation(
        "M13",
        "a queue timeout only fails a task that is still READY",
        "a claim racing a queue timeout",
        "engine/timeouts.py",
        [
            (
                "     WHERE id = :task_id\n       AND status = 'READY'\n",
                "     WHERE id = :task_id\n",
            ),
            (
                "    SELECT id, run_id FROM tasks\n     WHERE status = 'READY'\n",
                "    SELECT id, run_id FROM tasks\n     WHERE status IN ('READY', 'RUNNING')\n",
            ),
        ],
        [
            f"{T_DEAD}::test_queue_timeout_never_fires_for_a_claimed_task",
            f"{T_DEAD}::test_queue_timeout_races_with_claims_safely",
        ],
    ),
    Mutation(
        "M14",
        "retention skips runs locked by another transaction",
        "retention runs while an admin retries a run",
        "engine/retention.py",
        [
            (
                "         LIMIT :batch\n           FOR NO KEY UPDATE SKIP LOCKED\n    )\n"
                "    -- ON DELETE CASCADE",
                "         LIMIT :batch\n           FOR NO KEY UPDATE\n    )\n    -- ON DELETE CASCADE",
            )
        ],
        [f"{T_RET}::test_runs_locked_by_another_transaction_are_skipped"],
    ),
    Mutation(
        "M15",
        "retention only selects finished runs (status filter)",
        "active runs are never deleted",
        "engine/retention.py",
        [
            (
                "    WITH victims AS (\n        SELECT id FROM workflow_runs\n"
                "         WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')\n"
                "           AND completed_at <",
                "    WITH victims AS (\n        SELECT id FROM workflow_runs\n"
                "         WHERE completed_at <",
            )
        ],
        [f"{T_RET}::test_active_runs_are_never_deleted_however_old"],
    ),
    Mutation(
        "M16",
        "manual retry refused once the workflow deadline has passed",
        "manual retry of a deadline-failed run",
        "engine/control.py",
        [
            (
                '        if run["deadline_exceeded_at"] is not None or deadline_passed:',
                "        if False:",
            )
        ],
        [
            f"{T_RACE}::test_manual_retry_refused_once_the_deadline_has_passed",
            f"{T_DEAD}::test_user_cancel_still_ends_cancelled_and_deadline_runs_cannot_be_retried",
        ],
    ),
    Mutation(
        "M17",
        "a stale report for a deleted run is dropped, not retried",
        "worker reports after retention deleted its run",
        "engine/execution.py",
        [("    except IntegrityError:", "    except ZeroDivisionError:")],
        [f"{T_RET}::test_stale_result_for_a_run_deleted_by_retention_is_dropped"],
    ),
    Mutation(
        "M18",
        "a cancelled subprocess handler is signalled before any await",
        "process-isolated handler cancelled twice",
        "worker/executor.py",
        [
            (
                "            if proc.is_alive():\n                proc.terminate()\n"
                "            await asyncio.shield(asyncio.to_thread(_reap_process, proc))",
                "            await asyncio.sleep(0.02)\n            if proc.is_alive():\n"
                "                proc.terminate()\n"
                "            await asyncio.to_thread(_reap_process, proc)",
            )
        ],
        [
            "tests/unit/test_worker_units.py::test_cancelled_process_handler_never_outlives_cancellation"
        ],
    ),
    Mutation(
        "M19",
        "downstream de-duplication on the engine's operation key",
        "crash after an external side effect, before the result is saved",
        "handlers/builtin.py",
        [("            if use_key:", "            if False:")],
        [f"{T_PROC}::test_crash_after_side_effect_before_commit"],
        notes="Shows the at-least-once duplicate is real; the handler's de-dup prevents it.",
    ),
    Mutation(
        "M20",
        "the scheduler recovers expired leases",
        "worker crash (after claim, SIGKILL mid-task)",
        "engine/recovery.py",
        [
            (
                "    for cand in candidates:\n        result = await _recover_one(engine, cand)",
                "    for cand in []:\n        result = await _recover_one(engine, cand)",
            )
        ],
        [
            f"{T_PROC}::test_worker_sigkill_during_execution_is_recovered",
            f"{T_REL}::test_lease_expiry_recovered_by_scheduler_and_rerun",
        ],
    ),
    Mutation(
        "M21",
        "workers renew leases while executing (heartbeats)",
        "a task longer than its lease is not stolen from a healthy worker",
        "worker/runtime.py",
        [
            (
                "                continue\n            await self.heartbeat_once()",
                "                continue\n            pass",
            )
        ],
        [f"{T_PROC}::test_heartbeats_keep_a_task_longer_than_its_lease"],
    ),
    Mutation(
        "M22",
        "graceful shutdown drains in-flight tasks",
        "graceful shutdown (SIGTERM)",
        "worker/runtime.py",
        [
            (
                "        if not self.inflight:\n            return\n",
                "        if True:\n            return\n",
            )
        ],
        [f"{T_PROC}::test_graceful_shutdown_finishes_inflight_and_stops_claiming"],
    ),
    Mutation(
        "M23",
        "lease renewal tells the worker that its run is being cancelled",
        "cancellation during execution (cooperative handlers stop)",
        "engine/execution.py",
        [("r.status = 'CANCELLING' AS cancel_requested", "false AS cancel_requested")],
        [f"{T_REL}::test_cancel_run_with_ready_blocked_and_running_tasks"],
    ),
    Mutation(
        "M24",
        "run lookups are tenant-scoped",
        "tenant isolation",
        "engine/queries.py",
        [
            (
                "                    select(runs_t).where(\n"
                "                        and_(runs_t.c.id == run_id, runs_t.c.tenant_id == tenant_id)\n"
                "                    )",
                "                    select(runs_t).where(runs_t.c.id == run_id)",
            )
        ],
        [f"{T_API}::test_tenant_isolation"],
    ),
    Mutation(
        "M25",
        "a child is promoted only when all its dependencies succeeded",
        "dependency order",
        "engine/progression.py",
        [
            (
                "                           WHERE d.task_id = c.id))\n"
                "                    AND p.status <> 'SUCCEEDED')",
                "                           WHERE d.task_id = c.id))\n"
                "                    AND false)",
            )
        ],
        [
            f"{T_EXEC}::test_diamond_dependencies_and_run_output",
            "tests/integration/test_random_dags.py",
        ],
    ),
    Mutation(
        "M26",
        "dependency cycles are rejected before storage",
        "invalid graphs never execute",
        "domain/definition.py",
        [("    cycle = find_cycle(graph)", "    cycle = None")],
        [
            "tests/unit/test_definition.py",
            f"{T_API}::test_invalid_graphs_are_rejected_before_storage",
        ],
    ),
    Mutation(
        "M27",
        "a promoted retry restarts its queue-timeout clock (ready_at)",
        "time in RETRY_WAIT does not count towards the queue timeout",
        "engine/recovery.py",
        [
            (
                "           SET status = 'READY',\n               ready_at = statement_timestamp(),\n",
                "           SET status = 'READY',\n",
            )
        ],
        [f"{T_DEAD}::test_queue_timeout_restarts_after_retry_wait"],
    ),
]

# Mutants whose survival is explained (the protection is redundant by construction).
EXPECTED_SURVIVORS: dict[str, str] = {
    "M15": "Equivalent mutant: an active run has completed_at = NULL (CHECK constraint "
    "ck_workflow_runs_completed_at_iff_terminal), so `completed_at < cutoff` can never "
    "select it. The status filter is defence in depth and keeps the partial index usable.",
}


def apply(m: Mutation) -> tuple[Path, str]:
    path = SRC / m.file
    original = path.read_text()
    mutated = original
    for old, new in m.edits:
        n = mutated.count(old)
        if n != 1:
            raise SystemExit(f"{m.id}: edit anchor matches {n} times in {m.file}: {old[:60]!r}")
        mutated = mutated.replace(old, new)
    path.write_text(mutated)
    return path, original


def run_tests(tests: list[str]) -> tuple[int, str, float]:
    t0 = time.monotonic()
    proc = subprocess.run(
        [
            PY,
            "-m",
            "pytest",
            "-q",
            "-x",
            "-p",
            "no:cacheprovider",
            "-o",
            f"timeout={TEST_TIMEOUT}",
            *tests,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    out = proc.stdout + proc.stderr
    summary = next(
        (
            ln
            for ln in reversed(out.splitlines())
            if (" in " in ln and "=" not in ln) or ("passed" in ln or "failed" in ln)
        ),
        "",
    )
    first_fail = next(
        (ln.split(" - ")[0] for ln in out.splitlines() if ln.startswith(("FAILED", "ERROR"))), ""
    )
    return proc.returncode, (first_fail or summary).strip(), time.monotonic() - t0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("ids", nargs="*")
    ap.add_argument("--verify", action="store_true", help="only check that every edit applies")
    args = ap.parse_args()
    selected = [m for m in MUTATIONS if not args.ids or m.id in args.ids]

    if args.verify:
        for m in selected:
            path, original = apply(m)
            path.write_text(original)
        print(f"all {len(selected)} mutations apply cleanly (each anchor matches exactly once)")
        return

    baseline_rc, baseline, _ = run_tests(sorted({t for m in selected for t in m.tests}))
    if baseline_rc != 0:
        raise SystemExit(f"baseline (unmutated) tests do not pass: {baseline}")
    print(f"baseline: all targeted tests pass ({baseline})\n")

    for m in selected:
        path, original = apply(m)
        try:
            rc, detail, secs = run_tests(m.tests)
        finally:
            path.write_text(original)
        assert path.read_text() == original, f"{m.id}: restore failed"
        if rc == 5:
            raise SystemExit(f"{m.id}: no tests collected for {m.tests}")
        status = (
            "killed"
            if rc != 0
            else ("survived (expected)" if m.id in EXPECTED_SURVIVORS else "SURVIVED")
        )
        m.result = {"status": status, "detail": detail, "seconds": round(secs, 1)}
        print(f"{m.id} {status:20s} {secs:6.1f}s  {m.protection}\n      -> {detail}")

    unexpected = [m.id for m in selected if m.result["status"] == "SURVIVED"]
    write_reports(selected)
    print(
        f"\n{sum(m.result['status'] == 'killed' for m in selected)} killed, "
        f"{len(unexpected)} unexpected survivors {unexpected or ''}"
    )
    if unexpected:
        raise SystemExit(1)


def write_reports(ms: list[Mutation]) -> None:
    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    rows = "\n".join(
        f"| {m.id} | {m.protection} | {m.scenario} | `{m.file}` | "
        f"{'<br>'.join('`' + t.split('::')[-1].split('/')[-1] + '`' for t in m.tests)} | "
        f"**{m.result['status']}** | {m.result['seconds']} s |"
        for m in ms
    )
    notes = "\n".join(f"- **{k}**: {v}" for k, v in EXPECTED_SURVIVORS.items())
    (ROOT / "docs" / "mutation-results.md").write_text(
        "# Mutation results\n\n"
        f"Generated by `scripts/mutation_check.py` on {stamp}. Each row removes one protection "
        "from the source, runs the listed tests, and restores the file. *Killed* means a test "
        "failed, so the protection is really exercised. A hang beyond "
        f"{TEST_TIMEOUT} s counts as killed (lost liveness).\n\n"
        "| id | protection removed | failure-model scenario | file | tests | result | time |\n"
        "|---|---|---|---|---|---|---|\n"
        + rows
        + "\n\n## Survivors\n\n"
        + (notes or "None.")
        + "\n"
    )
    out = ROOT / "benchmarks" / "results" / "mutation-results.json"
    out.write_text(
        json.dumps(
            [
                {
                    "id": m.id,
                    "protection": m.protection,
                    "scenario": m.scenario,
                    "file": m.file,
                    "tests": m.tests,
                    **m.result,
                }
                for m in ms
            ],
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

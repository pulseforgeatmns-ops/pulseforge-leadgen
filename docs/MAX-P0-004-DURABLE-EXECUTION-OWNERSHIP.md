# MAX-P0-004 — Durable Execution Ownership

Status: implemented on a development branch; not deployed. This document incorporates the MAX-P0-003 handoff and review at authoritative `main` `c51ce3c`. No worker, cron, provider, tenant policy, production migration, or frontend path is enabled by this change.

## Design

`max_durable_execution_tasks` is the authoritative record for delegated work. It retains tenant and objective identity, optional mission/parent relation, specialist and scope, immutable input/dependency snapshots, deadlines, retry budget, checkpoints, result/evidence references, and every recovery/cancellation/escalation reason. It is deliberately separate from the P0-003 action ledger: task ownership answers **who may proceed**; the ledger answers **whether an external action has already been claimed or may be unknown**.

An accepted handoff is inserted before an acknowledgement is returned. The `(tenant_id, logical_key)` unique key and input fingerprint make repeated delegation replay the first task and reject a changed request. A worker is not started by this package; an explicitly authorised worker may claim a queued task later.

State sequence: `requested → queued → claimed → running → awaiting_verification → completed`. `blocked_dependency`, `recovery_required`, `failed`, `cancelled`, and `escalated` are explicit alternatives. Specialist completion only moves to `awaiting_verification`; evidence is required for `completed`.

## Ownership and recovery

Claims atomically allocate a token, increment a monotonic fence epoch, and set a database-clock lease. Every ownership-bound write (start, heartbeat, checkpoint, completion) is one compare-and-set on tenant, task, token, epoch, active state, unexpired lease **and unexpired deadline**, committed together with its event row. A stale, superseded, cancelled or past-deadline owner cannot write a checkpoint/result or enter the provider path.

Recovery never replays blindly. `recoverExpired` locks each due row and decides, in this order: an unresolved task-linked external action (`claimed`, `dispatch_attempted`, `outcome_unknown`) → `escalated` / `external_action_unresolved`; deadline passed → `escalated` / `deadline_exceeded`; recovery attempts ≥ retry budget → `escalated` / `retry_budget_exhausted`; otherwise `recovery_required` with the checkpoint preserved. `recovery_attempts` is incremented when a task *enters* recovery, so revalidation or requeueing can never reset the budget. Waiting tasks (`requested`, `queued`, `recovery_required`, `blocked_dependency`) whose deadline passed are escalated by the same sweep and are never claimable. `recovery_required` is not claimable: re-admission is explicit through `revalidateDependencies`, which compares persisted dependency references with supplied current references: drift becomes `blocked_dependency`; an exact match returns it to `queued`. A specialist completion reported while a task-linked action is still unresolved escalates in the same transaction (`external_action_unresolved`) instead of waiting indefinitely in `awaiting_verification`. Unknown provider outcomes remain in the P0-003 ledger and are reconciliation-only.

Cancellation locks the task row. If any task-linked action may already have reached a provider (`dispatch_attempted`, `outcome_unknown`, `provider_acknowledged`, `verified`), the effect cannot be recalled: the task ends `escalated` / `cancelled_after_dispatch` with the claim states in the event. Otherwise it ends `cancelled`.

### Dispatch fence (atomic with the P0-003 marker)

`OutboundExecutionAdapter` accepts an optional `durableTaskOwnership` binding. Where supplied, it fails closed without the P0-003 ledger, links the claim to `task_id`, checks `requireOwner` early, and passes a `taskFence` to `guardedDispatch`. `externalActionLedger.markDispatchAttempted({ …, taskFence })` then commits the dispatch marker in **one transaction** with a `SELECT … FOR SHARE` of the task row that re-checks token, epoch, active state, lease and deadline, and requires the claim's `task_id` to be that task. Cancellation, recovery and verification take `FOR UPDATE` on the same row, so either the marker commits first (and they observe the dispatch) or the marker is fenced and the claim is released as `abandoned_not_dispatched`. `guardedDispatch` refuses a task-linked claim without a matching fence before claiming. Verification rejects a task with a `claimed`, `dispatch_attempted`, or `outcome_unknown` claim. An ownership transfer therefore cannot produce a second logical send, and P0-004 does not create or bypass another action claim.

P0-003's R7 mission/prospect uniqueness remains authoritative. This layer never synthesizes a new outreach intent: task ownership is an additional fence, not permission to resend after a revision. Mission writes must continue to use `missionLockText(id)` through the P0-003 lease/session-lock protocol; a future task-to-mission commit adapter must validate its fence inside that same lease transaction.

### Database-enforced invariants (direct callers included)

- Tenant isolation by composite foreign keys: task parent `(tenant_id, parent_task_id)`, task events `(tenant_id, task_id)`, and ledger claims `(tenant_id, task_id)` all reference `max_durable_execution_tasks(tenant_id, id)`. A claim's task link is immutable once set, except that a never-delivered claim (`abandoned_not_dispatched`/`failed_definitive`) is re-attributed when another task reclaims it (so a successor task can send exactly once).
- No self-parent; `parent_task_id` is immutable, so the parent relation cannot form a cycle.
- Owner and lease are present exactly while the task is `claimed`/`running` (CHECK).
- At most one `claimed`/`running` task per `(tenant_id, mission_id)` (partial unique index); `claim` skips a busy mission.
- Accepted identity, tenant, input, dependency snapshot, deadline and retry budget are immutable; fence epoch and recovery attempts never decrease; a new owner token requires a new fence epoch; terminal rows are final (trigger).
- Task rows and events cannot be deleted or truncated; events cannot be updated.

## Persistence and migration

Apply `migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql` only after P0-003 and only with separate production approval. It is additive and re-runnable: two task tables, indexes (including the one-active-task-per-mission index), a nullable composite-keyed `task_id` link on the existing ledger with an immutability trigger, the task guard and append-only triggers above, and P0-003-style no-delete/no-truncate triggers for durable task evidence. It changes no existing rows or authorization policies. Old code ignores the new column and tables, so application rollback is a previous deploy while retaining the records. Do not drop them while unfinished tasks or action claims could require recovery.

## Acceptance evidence

All on disposable real PostgreSQL, included in `npm run test:max:reliability-core` (the hosted Max orchestration lane), alongside the restored P0-003 policy and tenant-mailbox suites:

- `test/maxDurableExecutionOwnershipPostgres.test.js`: durable/idempotent acceptance (input, deadline and budget bound to the key); two-pool concurrent claiming; stale-owner fencing after takeover; bounded recovery with preserved checkpoint; retry budget counted across revalidation; dependency drift; deadlines fencing a live owner and escalating waiting work; recovery escalation for unresolved external actions; cancel-before-dispatch (marker fenced, zero provider calls) and cancel-after-dispatch (escalated with claim states); the marker refusing a stale fence with no `beforeDispatch` check, and serializing behind a concurrent cancellation's row lock; one active task per mission under concurrent claimers and direct writes; **direct-SQL cross-tenant rejection** (claim INSERT/UPDATE, claim tenant change, event insert, parent insert, self-parent); immutable input, monotonic fence, final terminal states, append-only evidence; migration re-run over existing P0-003 rows.
- `test/maxDurableExecutionOwnershipAdapterPostgres.test.js`: the real `executeOutboundBundle` with a binding — a current owner sends once with task-linked claims and completes only through verification; a cancelled owner, and a binding whose fence is forged past the store check, reach no provider; no ledger → blocked.

The suites require `initdb`/`pg_ctl` and never skip. Process restart and backend termination are covered by P0-003's real PostgreSQL suite; P0-004's persisted records are reloaded by a fresh store over a second pool.

## Status against the approved P0-004 acceptance criteria

The approved proposal (this file at `main` `c51ce3c`, §5) defined 13 criteria across phases P1–P7. **This branch delivers P1 (store) plus the store-level parts of P3 (fencing), P4 (recovery rules) and P5 (cancellation), and the outbound dispatch fence.** It does not wire tasks into the router/TME, receipts, recovery CLI or Max presentation.

| # | Criterion (abridged) | Status |
|---|---|---|
| 1 | Every execution request creates one durable root task before specialist code | **Not in this branch** (P2: router/TME integration) |
| 2 | Max status derived from durable task state; never "done" early | **Not in this branch** (P6). Store separates `awaiting_verification` from evidence-backed `completed`. |
| 3 | CAS on state/fence/owner/DB-time lease; fence enforced in the TME commit | **Partial**: all store transitions and the outbound dispatch marker are fenced; TME commit fence and mission-lease narrowing are P3 integration. |
| 4 | At most one active task per mission per tenant, DB-enforced | **Met** (partial unique index) |
| 5 | Explicit recoverable state after a crash at every boundary | **Partial**: store and dispatch boundaries (claim/start/lease/marker) are covered; router/TME/receipt boundaries arrive with P2. |
| 6 | Deadline/stall never replays; external doubt → intervention | **Met** at store level for lease expiry, deadline, cancellation and completion (`escalated` is the intervention state). An `awaiting_verification` task whose action is attached later by a direct ledger caller is refused by `verify` but not swept. |
| 7 | Recovery tenant-scoped, plan-by-default, operator-attributed, audited | **Partial**: `recoverExpired` is tenant-scoped and evented, and re-admission is explicit (`recovery_required` is only re-queued by revalidation); `recover.cjs` extension (plan-by-default, `--operator`) not done. |
| 8 | Cycle-free dependencies, admission, failure propagation | **Partial**: parent tree cycle-free by DB; dependency drift blocks; no DAG admission/propagation. |
| 9 | Cancel before dispatch → cancelled, after → intervention with claim states | **Met** |
| 10 | Receipts, claims and tasks linked; task cannot complete with an unresolved claim | **Partial**: claims linked and enforced; receipts not linked. |
| 11 | Evidence non-deletable; tenant-scoped; no cross-tenant reads | **Met** (composite FKs, triggers, tenant-filtered reads) |
| 12 | Workers stay disabled | **Met**: no worker, timer, schedule or claimant |
| 13 | Existing suites green against `main`, no new baseline exceptions | Local: green. Hosted: see the PR / final report. |

State vocabulary differs from the proposal (`queued` ≈ `admitted`, `escalated` ≈ `awaiting_intervention`, `recovery_required` ≈ `interrupted`, `completed` after evidence ≈ `verified`); a presentation layer must map it without claiming more.

## Known limits

- Conservative escalation: a task whose only action is `claimed` (never dispatched) is escalated on lease expiry; an operator must release it. This favours no-replay over availability.
- A replayed action (same idempotency key already acknowledged under an earlier task) returns the original receipt; the successor task has no linked claim of its own, so its verification evidence must cite that receipt.
- Terminal rows are final: an `escalated` task stays escalated after an operator reconciles its action in the ledger. Closing an intervention is a later recovery-CLI step (new task or explicit resolution record), not an in-place update.
- A task-linked, never-delivered claim can only be reclaimed by a task-fenced caller; an untasked caller is blocked with `external_action_task_linked`.
- `claim()` returns `null` if it loses the one-active-per-mission race in that round, even if another mission has claimable work; the caller retries.
- A fence refusal mid-bundle aborts the bundle with `durable_task_fenced` (earlier sends are already recorded and ledger-protected), matching P0-003's handling of provider exceptions.

- No production worker is supplied or enabled. A later worker integration must use `claim`, revalidate dependencies, recover only bounded eligible tasks, and maintain heartbeats.
- The current dependency comparison is exact JSON reference equality. A domain adapter must persist canonical dependency versions; it must not silently reinterpret a dependency.
- Cancellation preserves P0-003 unresolved external-action claims. Operators must reconcile those effects separately.
- P0-004 recovery must extend `scripts/max-reliability/recover.cjs`, retain its tenant confirmation/operator attribution, and remain plan-only while workers are disabled; it must not create a second recovery command.
- There is no new UI. Existing structured Max interfaces can expose the task row and event stream.

## Controlled release

1. Keep workers, Levi, schedules, and outbound execution disabled.
2. Run the Render Shell report below; require `read_only: "on"`, P0-003 installed, and no unexpected catalog/migration discrepancies.
3. Obtain separate approval, apply P0-003 if absent, then P0-004 migration once. Re-run the report and require `durableExecutionOwnershipInstalled: true`.
4. Deploy manually with `PF_DISABLE_BACKGROUND_WORKERS` still set. Run read-only health and task-state inspection only.
5. Roll back by redeploying the previous app revision; retain both reliability tables. Never erase unresolved task or action-ledger records as part of rollback.

## Render Shell production schema report (read-only)

In the Render Shell for `pulseforge-api-migration`, at the checked-out application root:

```sh
node scripts/max-reliability/schemaReport.cjs > /tmp/max-reliability-schema-report.json && cat /tmp/max-reliability-schema-report.json
```

Expected result: one JSON document with `kind: "max_reliability_schema_report"`, `server.read_only: "on"`, catalog counts, `maxCore`, `reliabilityCoreInstalled`, `durableExecutionOwnershipInstalled`, tenant/mission aggregates, and no row content. Before either approved migration the corresponding installed flag is `false`; after it is applied the flag must be `true`. Any connection, privilege, timeout, or unexpected missing-migration result is a stop condition.

Required environment: `DATABASE_URL` must be present and point to the private Render PostgreSQL endpoint. Preserve `PF_DISABLE_BACKGROUND_WORKERS` as set, `PF_MIGRATION_RECEIVER=0`, no `MISSION_STORE_ALLOW_EPHEMERAL`, and keep `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED`, `MAX_INTERPRETATION_SHADOW`, and any Levi/outbound activation flag disabled. The report itself needs no provider credentials and performs no write.

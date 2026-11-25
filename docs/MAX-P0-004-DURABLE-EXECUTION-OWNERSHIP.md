# MAX-P0-004 — Durable Execution Ownership (proposal for approval)

Status: **proposal only.** Nothing here is implemented, migrated, deployed or enabled. Workers and external schedules stay disabled throughout.

## 1. Problem

MAX-P0-003 made individual *writes* and individual *external actions* safe: mission leases, compare-and-set, claims and receipts. It did not give Max a durable notion of **work**:

- A specialist execution exists only inside one HTTP request or chat turn. Its router audit lives in memory (`recordRouterAudit`). If the process dies mid-stage, the only traces are a rolled-back mission, possibly a claim, and possibly an `in_progress` receipt. Nothing says *what was being done, by whom, and how far it got*.
- Max cannot honestly distinguish a request that was **requested**, **started**, **completed**, **independently verified**, **interrupted**, or **awaiting intervention**. `execution.state` is computed per request and then forgotten.
- Ownership of long work is a database **transaction**: the mission lease is held for the whole specialist run, up to the 10-minute idle bound. That is correct for mutual exclusion but the wrong tool for long-running ownership. It pins a connection, cannot heartbeat, and cannot be inspected or cancelled.
- Chat requests have no identity, so a duplicated "Approved." is a new request.
- Multi-step work (discovery → prioritization → copy → capacity → execution) has no durable dependency record. A crash between steps loses the chain.

The repository already contains a proven pattern for this: governed stage scheduling (`docs/reliability/durable-stage-scheduling.md`, `services/governedOutboundStage*`, migration `2026-10-21-governed-stage-scheduling.sql`). It provides claim → start → finalize with a fence, database-time leases, quarantine instead of replay, and a supervised child process with deadlines. MAX-P0-004 generalises that pattern to Max specialist execution. It does **not** replace the outbound stage scheduler; that becomes one task kind later, if desired.

## 2. Status vocabulary Max must distinguish

| Task state | Meaning | Max may say |
|---|---|---|
| `requested` | Durable record exists; not yet admitted (dependencies, capacity, approval) | "Requested, not started" |
| `admitted` | Eligible; waiting for an owner | "Queued" |
| `running` | Owner holds a live lease and has committed `started` | "In progress (since …)" |
| `completed` | Handler finished; internal commit verified (`committed_internal`) | "Done (internal)" |
| `verified` | Completion confirmed by independent evidence per intent (provider ids, live discovery, …; reuses `ExecutionEvidence`) | "Done and verified" |
| `interrupted` | Owner lost (lease expired, crash, deadline) **with no external action in doubt**; re-admission allowed within an attempt budget | "Interrupted — will retry" / "Interrupted — needs a decision" |
| `awaiting_intervention` | Unknown external outcome, exhausted attempts, failed dependency, deadline hit after a dispatch, or a cancellation that arrived after dispatch | "Needs you: …" with the reason |
| `failed` | Definitively failed; nothing in doubt | "Failed: …" |
| `cancelled` | Cancelled before any external dispatch | "Cancelled" |

Rules:
- Max never presents `requested`, `admitted` or `running` as done.
- Max never presents `completed` as verified when the intent requires external evidence.
- `awaiting_intervention` always carries a machine-readable reason and the evidence references needed to resolve it.

## 3. Proposed architecture

### 3.1 Data model (one additive operator migration, never startup DDL)

```
max_tasks
  tenant_id, task_id (PK tenant_id, task_id)
  kind              -- e.g. amo_stage, specialist_run, outbound_batch
  mission_id, intent, specialist, stage
  parent_task_id    -- root task = one request
  request_receipt   -- (scope, idempotency_key) → max_request_receipts
  request_fingerprint, correlation_id, requested_by
  state             -- §2, CHECK-constrained
  attempt, max_attempts
  fence             -- monotonic; bumped on every (re)claim
  owner_token, lease_expires_at, heartbeat_at    -- database time only
  deadline_at       -- bounded execution (per kind)
  started_at, finished_at
  cancel_requested_at, cancel_requested_by
  outcome_code, outcome_detail (redacted JSONB), evidence_refs (JSONB)
  intervention_reason, escalated_at
  created_at, updated_at
  UNIQUE (tenant_id, mission_id) WHERE state IN ('admitted','running')   -- one active stage per mission

max_task_dependencies (tenant_id, task_id, depends_on_task_id, required_state)  -- DAG, cycle-checked on insert
max_task_events       (append-only, redacted: from_state, to_state, fence, actor, reason, at)  -- DELETE/TRUNCATE forbidden by trigger, as in P0-003
max_external_action_claims.task_id      -- nullable column added to the P0-003 table (additive)
```

### 3.2 Ownership, leases, heartbeats, fencing

- **Claim**: a short transaction sets `owner_token` and bumps `fence`, with `lease_expires_at = now() + lease`. It succeeds only from `admitted` (or from `interrupted` when re-admission is allowed).
- **Start**: CAS `admitted → running` requiring the current `owner_token`, `fence` and an unexpired lease, all checked in **database time**. The handler is invoked only after `start` commits. If the start acknowledgement is lost, the handler is not invoked (same rule as stage scheduling).
- **Heartbeat**: renews the lease and also requires the current fence. A late heartbeat from a superseded owner fails, and that owner must stop.
- **Fencing reaches the writes**: TME `persistDurable` receives `{ taskId, fence }` and verifies `max_tasks.fence` in the same lease transaction as the mission commit. A zombie owner whose lease expired can therefore never commit a stage, even if it still holds a connection.
- **Mission lease narrowed**: with task ownership in place, the MAX-P0-003 mission lease is held only around the stage commit (seconds), not across the specialist run. This removes the long idle-in-transaction connection, and mutual exclusion is preserved by the `one active task per mission` index plus the fence check.
- No connection is held while a handler runs, except the brief commit lease.

### 3.3 Bounded execution and stall detection

- Each kind declares `deadline` and `lease` (defaults proposed: discovery 10 min, copy/capacity 5 min, outbound batch 20 min; lease 60 s, heartbeat 20 s).
- **Deadline exceeded** (heartbeat-side or observer-side): the task stops renewing and moves to `interrupted` if no claim of this task is in `dispatch_attempted`/`outcome_unknown`; otherwise to `awaiting_intervention`. The underlying operation is never replayed via `Promise.race`.
- **Stall detection** is the same rule applied by an observer to `running` tasks whose lease expired. Database lease expiry covers a blocked event loop.
- An optional supervised child process (reusing `governedOutboundStageSupervisor`) is available for kinds whose work must be killable. It is not required for phase 1.

### 3.4 Restart recovery

- Recovery is an operator-run command, like the P0-003 `recover.cjs`. It does not run on boot while `PF_DISABLE_BACKGROUND_WORKERS=1`. It is tenant-scoped, plan-by-default, and requires `--operator` and `--confirm-tenant`.
- Rules:
  - `running` with an expired lease and no claims in doubt → `interrupted`; re-admissible if `attempt < max_attempts`, else `awaiting_intervention`.
  - Any claim in doubt → `awaiting_intervention`.
  - `admitted` older than the bound → stays `admitted`, reported.
  - Receipts linked to interrupted tasks → `interrupted`. Replays answer with the task state, never re-run.
- Nothing is auto-retried while workers are disabled. Re-admission is an explicit operator action, or a later approved worker.

### 3.5 Dependencies

- A task is admitted only when every dependency is in its `required_state` (default `completed`; `verified` for steps that consume external evidence).
- A dependency in `failed`, `cancelled` or `awaiting_intervention` moves dependents to `awaiting_intervention` (reason `dependency_blocked`). Dependents are never silently skipped.
- Cycles are rejected at insert (recursive CTE in the same transaction).

### 3.6 Cancellation and escalation

- `cancel_requested_at` is set by an authorised operator (admin/manager) and checked at handler step boundaries.
- Before any dispatch marker: → `cancelled`.
- After a dispatch marker: the external action cannot be recalled. The task finishes its accounting and ends in `awaiting_intervention` (reason `cancelled_after_dispatch`), with the claim states attached.
- Escalation: entering `awaiting_intervention` writes one `agent_actions` row (`action_type = 'max_task_intervention'`, tenant-scoped, redacted) and sets `escalated_at`. Max surfaces it in briefings and chat. No notification channel is added in phase 1.

### 3.7 Integration with MAX-P0-003

| P0-003 primitive | Integration |
|---|---|
| Request receipt | 1:1 with a root task. The receipt response carries `taskId`. A replay of a still-running request returns the live task state instead of 409. |
| Idempotency key | Chat gains a client message id (`X-Client-Message-Id` / Max UI) → receipt → task. This closes the duplicated-"Approved." gap. |
| External-action claim | Carries `task_id`. A task cannot reach `completed` while any of its claims is `claimed`/`dispatch_attempted`/`outcome_unknown`. It reaches `verified` only when every claim is `provider_acknowledged`/`verified` (or `failed_definitive`, with the evidence rules of the intent). |
| Mission lease + CAS | Lease narrowed to the commit (§3.2). CAS unchanged. The task fence is checked in the same transaction. |
| Recovery CLI | Extended with task rules (§3.4); one audit trail. |
| Reliability audit | Task events written alongside, with the same redaction. |

## 4. Implementation scope (phased; each phase separately reviewable, workers stay off)

| Phase | Delivers | Behaviour change |
|---|---|---|
| **P1 Store** | Migration (3 tables + `task_id` column + triggers), `services/maxTasks/` store with CAS transitions, state machine, dependency cycle check, redaction. PostgreSQL tests. | none (library only) |
| **P2 Record** | Router/TME create and advance a task for every execution request, run **inline** as today. Receipts link to tasks; chat client message ids. | adds durable records; execution path unchanged |
| **P3 Own** | Claim/start/heartbeat/fence; fence checked in the TME commit; mission lease narrowed to the commit; per-kind deadlines; stall → interrupted/awaiting_intervention. | ownership enforced inline |
| **P4 Recover** | Recovery CLI task rules; operator re-admission; escalation `agent_actions` rows. | operator-run only |
| **P5 Depend / Cancel** | DAG admission, failure propagation, cooperative cancellation. | |
| **P6 Present** | Max reads task state for "what's happening / what's done / what needs me", with verified-vs-completed honesty. | presentation |
| **P7 (optional, separately approved)** | A background executor that claims `admitted` tasks. Default-off; refuses to start while `PF_DISABLE_BACKGROUND_WORKERS=1`. | none until enabled |

Also carried into this package from the MAX-P0-003 review:
- Move the High-relevance session-lock sites (governed outbound store, replies and control dispatch; tenant outreach executor; Emmett autosend) onto `withWorkerAdvisoryLock`-style destroy-on-unlock-failure.
- Bring Paige social publishing and Twilio/Bland sends onto the claim ledger.
- Implement the R7 decision (re-send after revision).
- Raise the L1 test deadline.

Out of scope: enabling any worker, schedule, outbound send, research cycle or new specialist capability; changing tenant authorization policy; production migration or deployment.

## 5. Acceptance criteria

1. Every execution request (REST, Mission Workspace, chat, governed executor, scripts) creates exactly one durable root task before any specialist code runs. Concurrent identical requests yield one task.
2. Max's reported status for any task is derived from durable task state and never claims done for `requested`/`admitted`/`running`. It reports `verified` only with the intent's independent evidence.
3. All state transitions are compare-and-set on (state, fence, owner, unexpired database-time lease). A superseded owner cannot heartbeat, start, finish or commit a mission stage (fence enforced inside the TME commit transaction).
4. At most one active (`admitted`/`running`) task per mission per tenant, enforced by the database.
5. A crash at every boundary (before claim, claim/start, start/handler, mid-handler, handler/commit, commit/finish, finish/receipt) leaves an explicit, recoverable state. Proven by multi-process PostgreSQL failure injection.
6. Deadline or stall never replays work. A task with an external action in doubt always ends `awaiting_intervention`, never `interrupted` or retried.
7. Recovery is tenant-scoped, plan-by-default, operator-attributed and audited. Re-admission is explicit and bounded by `max_attempts`.
8. Dependencies are cycle-free, admitted only when satisfied, and failures propagate to `awaiting_intervention` with a reason.
9. Cancellation before dispatch ends `cancelled` with no external call. Cancellation after dispatch ends `awaiting_intervention` with claim states attached.
10. Receipts, claims and tasks are linked. A replay returns the original receipt plus live task state. A task cannot complete while any claim is unresolved.
11. Task events and claims cannot be deleted or truncated (DB triggers). Every row is tenant-scoped, and cross-tenant reads return nothing.
12. Workers stay disabled. No phase introduces a timer, schedule or background claimant without separate approval. P1–P6 run entirely inline or operator-invoked.
13. Existing suites stay green against the `main` baseline, with no new ratchet or baseline exceptions. Load-sensitive tests are tracked separately with reproduction evidence.

## 6. Risks and open questions for approval

- **Inline vs async:** P2–P6 keep execution inline (request-bound) to avoid enabling workers. Long discovery runs remain bounded by HTTP timeouts until P7 is approved.
- **Mission-lease narrowing (P3)** changes the MAX-P0-003 locking topology. It requires the fence check to be in place first, and the same multi-process tests.
- **Chat client message ids** need a small Max UI change (one id per send, reused on retry).
- **Deadline defaults** need confirmation per specialist from production timing data (the schema report and logs).
- **R7 policy** (re-send after a revision for non-governed tenants) must be decided before P2 links outbound claims to tasks.

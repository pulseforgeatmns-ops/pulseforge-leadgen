# MAX-P0-004 — Durable Execution Ownership

Status: Phase 1 (store, fencing, dispatch fence) merged to `main` in #1164 (`a51facd9`). Phases P2–P6 (router/TME integration, mission-commit fencing, receipt linkage, recovery CLI, task-aware presentation) are implemented on a development branch and are **default-off** (`MAX_DURABLE_TASKS`). Not deployed. No worker, cron, provider, tenant policy, production migration, or frontend path is enabled by this change.

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

## Router, TME, receipts, recovery and presentation (P2–P6)

Everything below is active only when the router context carries a task store. Production surfaces pass one only with `MAX_DURABLE_TASKS=true` and a pool (`DurableTaskRouting.resolveDurableTasks`); otherwise behaviour is byte-for-byte unchanged.

**P2 — router ownership** (`packages/acquisition-mission/DurableTaskRouting.js`, wired in `routeExecutionRequest`). After the router's authorization, research-planning and synthetic-evidence gates, and before any specialist code:
1. `accept` the request as a task. The logical key is `receipt:<scope>:<Idempotency-Key>` for keyed REST requests, else `cer:<requestId>`. If the key already exists and is not `queued`, the recorded task is returned (`durable_task_replay`) and nothing is dispatched. A `queued` task (accepted, never claimed) is resumed with its recorded deadline.
2. `claimTask` that specific task: new token, fence epoch + 1, DB-clock lease, at most one active task per mission. If the mission already has an active task the request is refused (`durable_task_mission_busy`, HTTP 409) and the new task is cancelled (`mission_has_active_task`) rather than left queued.
3. `start`, then run the dispatch inside `runWithTaskFence` (`services/maxReliability/taskExecutionContext.js`, AsyncLocalStorage). A heartbeat renews the lease while the request runs and is cleared when it ends. This is not a claimant: it only renews a lease the request already holds.
4. `finish` from the router's execution evidence: `committed_internal` / `completed_verified` / `already_executed` → `reportCompletion` + `verify` with the evidence (state, level, transaction id) → `completed`; `failed` / `blocked` → `failed`; anything else → `awaiting_verification`. A request that throws → `fail`. An unresolved task-linked action turns either into `escalated`. If ownership was lost meanwhile, the durable state is reported as it is.
Router tasks have `retryBudget: 0`: an interrupted inline request is escalated for an operator, never resumed automatically. The chained autonomous-discovery step runs under its parent's task and fence. Surfaces wired: `executeCanonical` (REST `/execute`) and Max chat (`AcquisitionMissionExecution`).

**P3 — mission-commit fencing.** Every leased mission write goes through the lease branch of `persistStageCommit`, which now runs `assertTaskFenceOnClient` (`SELECT … FOR SHARE` on the task row, same tenant as the mission) inside the lease transaction before writing. A superseded, cancelled, recovered or past-deadline owner therefore cannot make mission state durable. Cancellation and recovery lock the row `FOR UPDATE`, so they either wait for the commit or make it fail. Under a fence, the unlocked write path is refused, and TME refuses a stage without the mission lease before the specialist runs. The outbound adapter picks up the context fence automatically: claims are task-linked and the P0-003 dispatch marker is fenced. Fail-open fix from Phase 1 retained: a lost mission lease rolls the stage back.

**P4 — receipts and recovery.** `max_request_receipts.task_id` (composite FK to the task, immutable once set) is linked by the receipt owner after `accept` and before any specialist runs; if linking fails the task is cancelled and nothing runs. Replays, `request_in_progress` and `request_outcome_unknown` responses carry `durableTask` (live state). `scripts/max-reliability/recover.cjs` gains `--tasks` (plan, read-only), `--tasks --apply`, `--revalidate-task <id> --dependency-refs <json>` and `--cancel-task <id>`. Every mutation needs `--confirm-tenant` equal to `--tenant` and `--operator`, which is recorded as `operator:<id>` in the task event (`services/maxReliability/taskRecovery.js`). Output is redacted.

**P6 — presentation** (`packages/max/workspace/DurableTaskStatus.js`). Status comes only from the task row. Only `completed` is "done"; `awaiting_verification` says "not verified"; `escalated` and `recovery_required` name the reason and the operator action; unknown states are reported as unknown. Max chat turns carry `metadata.durableTask` and a visible notice whenever the task is not verifiably done. `GET /api/v1/amo/missions/:id/tasks` is read-only and tenant-scoped (`enabled:false` when off; 503 if the schema is missing).

## No ownership bypasses (criterion 1, PR A)

With `MAX_DURABLE_TASKS=true`, ownership is enforced at four choke points (`services/maxReliability/executionOwnershipPolicy.js`). With the flag unset, none of this changes behaviour.

| Choke point | Without a live task fence |
|---|---|
| `routeExecutionRequest` | refused (`durable_task_owner_required`, audited as `blocked_no_task_owner`) unless the caller passes a task store. The only alternative is `offline_evaluation`: no pool, nothing persisted, discovery only with an injected Scout runner. |
| `executeMissionStage` (TME) | refused before the specialist runs (offline evaluation excepted, never with a lock pool) |
| `OutboundExecutionAdapter` | refused before any claim or provider call; **no exemption covers a send** |
| `persistStageCommit` | refused unless a closed **write** exemption applies; even then only through the mission lease, and the exempted write is recorded in `max_reliability_audit` **in the same transaction**. The unlocked path is always refused. |

**Write exemptions (closed allowlist; no specialist, no external effect):**
- `mission_creation`: `runtime.create`.
- `operator_restriction_tightening`: monotonic constraint tightening (`AcquisitionOwnership`).
- `inbound_observation`: provider events and Riley reply interpretation.

The structural test pins each exemption to its call sites.

**Every other caller is task-owned:**
- **REST and chat:** unchanged from P2.
- **Governed adapters:** `governed-prepare:<program>:<day>:<mission>:<intent>:<version>`, `governed-approve:<program>:<mission>:<snapshotHash>`, `governed-send:<program>:<envelope>:<candidate>`, and envelope completion as `governed-complete:<envelope>`.
- **Anchor scoped-resume:** `anchor-crm-prep:<operationId>:<step>:<intent>`.
- **Operator scripts:** `operatorRunKey(script, parts)`, where the parts are mission, intent, the prepared-artifact revision read before the call, prospect and limits. The key never contains a run id or a time, so a retry of the same logical execution replays its task; TME rolls back on a crash, so the inputs are unchanged. The repair script `auditAnchorCapacitySupersession` goes through the mission lease when enforced.
- **Authorization amendments:** `amendment:<mission>:<amendmentId>`.
- **Outcome-learning evaluation:** `outcome-learning:<mission>:<version>`.
- **Cancellation persistence:** runs inside the router task through `afterDispatch`.
- **Runtime mutations** (`contribute`, `progress`, `runAutonomousProgression`, legacy `persistSideEffects`): `ownedMissionWork`, one task per call, like an unkeyed router request.
- **Nested work:** inside an already task-owned execution, nested work reuses the existing fence rather than opening a second task.

**Attempts (independent review, important).** A stable key replays its latest attempt, unless that attempt ended `failed` or `cancelled`. Those two states mean no effect is possible, because the store escalates instead whenever a task-linked action may have reached a provider. In that case a retry of the same logical execution opens `<key>#attempt-N`.
- `completed` replays as the success.
- `escalated`, `recovery_required`, `awaiting_verification` and active tasks replay; they need an operator or are still in flight.
- After 20 no-effect attempts the key requires operator review (`durable_task_attempts_exhausted`).

So a key is never spent by a transient collision (`mission_has_active_task`) or by a clean failure, and concurrent retries still converge on one attempt through the unique key.

**Exemption scope (independent review, important).** Each exempt write is checked against the durable row inside the lease transaction:
- `mission_creation`: only a mission that does not exist yet.
- `operator_restriction_tightening`: only `authorizationConstraints`, monotonic (every prohibition and review-only kept, at least one added).
- `inbound_observation`: none of stage, status, plan-cancelled, pending decision, plan, objective, tenant or constraints may change; the write may only add observations, outcomes and events.

Anything riding along is refused (`durable_task_exemption_scope`).

Inbound events (provider observations, Riley reply interpretations) persist through `runtime.persistInbound`. It compares the guarded fields before and after the event was applied:
- if nothing changed, the write is the audited `inbound_observation` exemption;
- if applying the event also advanced the mission (an outcome auto-advancing OBSERVE→LEARN, a refreshed decision or status), the write is task-owned work.

So a legitimate inbound write is never refused, and a mission advance is never recorded as an observation. With durable tasks off both reduce to the original `persistMissionState`.

**Fence scope.** A fence carries its mission:
- a write to another mission under it is refused unless an exemption applies;
- `runTaskOwnedWork` inherits a fence only for the same mission, so nested work on another mission takes its own task.

The offline exemption admits discovery only with a Scout runner explicitly marked `offlineFixture`.

**Structural enforcement.** `test/maxDurableOwnershipStructure.test.js` scans production code for every caller of `routeExecutionRequest`, `executeMissionStage`, `persistMissionState`, `persistStageCommit`, `executeOutboundBundle` and `guardedDispatch`. It requires each file to be classified and to still carry its ownership wiring, and it pins every exemption to its permitted call sites. An unclassified caller, or one that loses its wiring, fails CI. Both failure modes were checked by mutation.

## Persistence and migration

Apply `migrations/2026-11-25-max-p0-004-durable-execution-ownership.sql` only after P0-003 and only with separate production approval. It is additive and re-runnable: two task tables, a composite-keyed immutable `task_id` on request receipts, indexes (including the one-active-task-per-mission index), a nullable composite-keyed `task_id` link on the existing ledger with an immutability trigger, the task guard and append-only triggers above, and P0-003-style no-delete/no-truncate triggers for durable task evidence. It changes no existing rows or authorization policies. Old code ignores the new column and tables, so application rollback is a previous deploy while retaining the records. Do not drop them while unfinished tasks or action claims could require recovery.

## Acceptance evidence

All on disposable real PostgreSQL, included in `npm run test:max:reliability-core` (the hosted Max orchestration lane), alongside the restored P0-003 policy and tenant-mailbox suites:

- `test/maxDurableExecutionOwnershipPostgres.test.js`: durable/idempotent acceptance (input, deadline and budget bound to the key); two-pool concurrent claiming; stale-owner fencing after takeover; bounded recovery with preserved checkpoint; retry budget counted across revalidation; dependency drift; deadlines fencing a live owner and escalating waiting work; recovery escalation for unresolved external actions; cancel-before-dispatch (marker fenced, zero provider calls) and cancel-after-dispatch (escalated with claim states); the marker refusing a stale fence with no `beforeDispatch` check, and serializing behind a concurrent cancellation's row lock; one active task per mission under concurrent claimers and direct writes; **direct-SQL cross-tenant rejection** (claim INSERT/UPDATE, claim tenant change, event insert, parent insert, self-parent); immutable input, monotonic fence, final terminal states, append-only evidence; migration re-run over existing P0-003 rows.
- `test/maxDurableExecutionOwnershipAdapterPostgres.test.js`: the real `executeOutboundBundle` with a binding — a current owner sends once with task-linked claims and completes only through verification; a cancelled owner, and a binding whose fence is forged past the store check, reach no provider; no ledger → blocked.

- `test/maxDurableExecutionE2EPostgres.test.js` (P2–P6): router + real TME + real mission persistence + task store + receipts + recovery. It covers:
  - completion only with evidence, and replay never re-dispatching;
  - takeover and cancellation during execute rolling the stage back with no durable write;
  - task-owned writes refusing the unlocked path, a stage without the lease never running, and a foreign-tenant fence being refused;
  - one active task per mission;
  - an owner process killed with `SIGKILL` mid-specialist, then restart, plan-only recovery, attributed apply and no mission write;
  - receipts linked before dispatch, with live task state on replay, an immutable link and cross-tenant rejection;
  - operator revalidate/cancel, tenant-scoped;
  - default-off resolution.
  A mutation check (fence check removed from `persistStageCommit`) makes the takeover, cancellation and foreign-fence tests fail.
- `test/maxDurableTaskStatus.test.js` (P6): only `completed` is done; unknown never success; notices on every non-done turn.
- The adapter suite also proves that the context fence alone task-links and fences sends, and that a stale or foreign fence reaches no provider.

The suites require `initdb`/`pg_ctl` and never skip. Process restart and backend termination are covered by P0-003's real PostgreSQL suite; P0-004's persisted records are reloaded by a fresh store over a second pool.

## Status against the approved P0-004 acceptance criteria

The approved proposal (`main` `c51ce3c`, §5) defined 13 criteria across phases P1–P7. Phase 1 (#1164) delivered the store and fences. This branch adds P2–P6, all **default-off** (`MAX_DURABLE_TASKS`). P7 (a background executor) is not part of P0-004's authorized scope and is not implemented.

| # | Criterion (abridged) | Status |
|---|---|---|
| 1 | Every execution request creates one durable root task before specialist code; concurrent identical requests yield one task | **Met when enabled**: every router caller (REST, chat, governed adapters, Anchor scoped-resume, operator scripts) passes a task store with a stable key. Router, TME and send refuse unowned work, and mission writes outside a task are limited to three audited write exemptions. A structural CI test inventories every caller. |
| 2 | Max status derived from durable task state; never "done" early | **Met when enabled**: `DurableTaskStatus` + chat notice + `GET …/tasks`. |
| 3 | CAS on state/fence/owner/DB-time lease; fence enforced in the TME commit | **Met**: every store transition, the dispatch marker and every leased mission commit re-check the fence in PostgreSQL. *Mission-lease narrowing (§3.2) is deliberately not done*: it changes P0-003's lock topology and needs its own approval and multi-process proof. |
| 4 | At most one active task per mission per tenant, DB-enforced | **Met** (partial unique index; the router refuses a second request). |
| 5 | Explicit recoverable state after a crash at every boundary | **Met for the tested boundaries**: owner killed mid-specialist (separate process, `SIGKILL`) → expired lease → plan → attributed recovery, with no mission write; takeover and cancellation mid-execute → commit refused; a failure after dispatch still ends ownership. Other boundaries (claim/start, finish/receipt) fall to the same lease-expiry rule but are not individually injected. A task left `queued` by a crash between accept and claim is resumed only by a direct router call reusing its key: through `/execute` its receipt becomes `interrupted`, which is never re-run, and with no worker a re-queued task is not picked up automatically. An operator cancels it (`recover.cjs --cancel-task`). |
| 6 | Deadline/stall never replays; external doubt → intervention | **Met**: router tasks have no retry budget, so interruption escalates. Unresolved actions escalate on failure, completion, recovery and cancellation. Actions that already reached a provider escalate on failure (`failed_after_dispatch`, e.g. a send acknowledged before the mission commit rolled back) and on cancellation. |
| 7 | Recovery tenant-scoped, plan-by-default, operator-attributed, audited | **Met**: `recover.cjs --tasks` (plan / apply / revalidate / cancel), tenant confirmation, operator attribution in task events. |
| 8 | Cycle-free dependencies, admission, failure propagation | **Partial**: the parent tree is cycle-free by DB and dependency drift blocks; there is no DAG admission or failure propagation. Chained steps share the parent task instead of becoming dependents. |
| 9 | Cancel before dispatch → cancelled, after → intervention with claim states | **Met** (store and E2E). |
| 10 | Receipts, claims and tasks linked; replay returns live task state; no completion with an unresolved claim | **Met**: receipt `task_id` linked before dispatch; replays and conflicts carry `durableTask`. |
| 11 | Evidence non-deletable; tenant-scoped; no cross-tenant reads | **Met** (composite FKs on claims, events, parents and receipts; triggers; tenant-filtered reads). |
| 12 | Workers stay disabled | **Met**: no worker, schedule or claimant. The only timer is a request-scoped lease heartbeat, cleared on every exit path (success, failure, or a failure after dispatch). |
| 13 | Existing suites green against `main`, no new baseline exceptions | Local: green. Hosted: see the PR. |

State vocabulary differs from the proposal (`queued` ≈ `admitted`, `escalated` ≈ `awaiting_intervention`, `recovery_required` ≈ `interrupted`, `completed` after evidence ≈ `verified`). `DurableTaskStatus` maps it for operators without claiming more.

## Known limits

- A retry after a `failed` attempt runs again. That is right for external effects: none happened, and the ledger deduplicates sends anyway. But paid provider work done before a rolled-back stage (Scout/Places, Paige generation) is paid for again on the retry.
- When two callers retry the same key concurrently, only one attempt is created. The loser usually gets `durable_task_key_reused` (422) rather than a replay. REST is serialized by its receipt.
- The offline Scout marker (`runScout.offlineFixture`) is declared by the caller. It is a reviewed marker, pinned to one script by the structural test, not a cryptographic guarantee.

- Tenant learning rows (`persistLearning`/`persistPrediction`/`persistOutcomeEvaluation`/`persistOutcomeLearning`) are written on the pool after the mission lease commits. Per-send outbound records are written by the Emmett stage, inside the router's fenced execution. Neither is a core mission row, and neither is routed through the exemption audit.
- Provider observation skips its OBSERVE/LEARN progression while another task is active on the mission (`mission_busy` is recorded as `observeBlocked`/`learnBlocked`). It is retried on the next event, as it already was when the mission lock was held.

- Activation (`MAX_DURABLE_TASKS=true`) requires the P0-003 and P0-004 migrations; it is an operator decision and not part of this change.
- `persistStageCommit` still has a pre-existing unlocked fallback when a caller holds the process's mission lock but passes neither `lease` nor `skipGlobalLock`. No production caller reaches it, and under a task fence it is refused.
- A task-linked keyed request whose dispatch fails before anything ran (e.g. `tme_transaction_overlap`) keeps its receipt as a failure (`retryableWithNewKey: true`). The task and its logical key are permanent, so the same Idempotency-Key only ever replays it; the client retries with a new key.
- Two concurrent callers passing the same explicit `durableTaskKey` outside the receipt path (scripts) can race at acceptance. Verified (`verify:` test, 5 rounds × 3 callers):
  - at most one execution;
  - each loser gets `durable_task_key_reused` (422; the accepted deadline differs) or `durable_task_mission_busy`, instead of a replay;
  - a loser never cancels the winner's task: after a failed claim it cancels with an atomic, conditional `cancelIfQueued`, which is a no-op once anyone has claimed the task. An earlier read-then-cancel had a narrow window, found in independent review and fixed;
  - keys are tenant-scoped.
  REST is serialized by its receipt.
- A new-key retry after a definitive pre-dispatch failure is a new task, but it cannot bypass ownership or deduplication. Verified (`verify:` test):
  - while any task of the mission is active, it is refused (`durable_task_mission_busy`);
  - a send already acknowledged under the earlier task is replayed by the P0-003 ledger, which keys on the action, not the task;
  - a revised artifact for the same mission and prospect is blocked by R7;
  - the delivered claim stays attributed to the task that sent it.
- Code spawned inside a task-owned dispatch inherits the fence (AsyncLocalStorage). That is intended: anything it commits is fenced by the same owner.

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
4. Deploy manually with `PF_DISABLE_BACKGROUND_WORKERS` still set and `MAX_DURABLE_TASKS` unset. Run read-only health and task-state inspection only.
5. Enabling `MAX_DURABLE_TASKS=true` is a separate, explicitly approved step after both migrations. It changes no authorization: requests still pass every existing gate and only gain ownership, fencing and durable status.
6. Roll back by unsetting `MAX_DURABLE_TASKS` (or redeploying the previous app revision); retain both reliability tables. Never erase unresolved task or action-ledger records as part of rollback.

## Render Shell production schema report (read-only)

In the Render Shell for `pulseforge-api-migration`, at the checked-out application root:

```sh
node scripts/max-reliability/schemaReport.cjs > /tmp/max-reliability-schema-report.json && cat /tmp/max-reliability-schema-report.json
```

Expected result: one JSON document with `kind: "max_reliability_schema_report"`, `server.read_only: "on"`, catalog counts, `maxCore`, `reliabilityCoreInstalled`, `durableExecutionOwnershipInstalled`, tenant/mission aggregates, and no row content. Before either approved migration the corresponding installed flag is `false`; after it is applied the flag must be `true`. Any connection, privilege, timeout, or unexpected missing-migration result is a stop condition.

Required environment: `DATABASE_URL` must be present and point to the private Render PostgreSQL endpoint. Preserve `PF_DISABLE_BACKGROUND_WORKERS` as set, `PF_MIGRATION_RECEIVER=0`, no `MISSION_STORE_ALLOW_EPHEMERAL`, and keep `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED`, `MAX_INTERPRETATION_SHADOW`, and any Levi/outbound activation flag disabled. The report itself needs no provider credentials and performs no write.

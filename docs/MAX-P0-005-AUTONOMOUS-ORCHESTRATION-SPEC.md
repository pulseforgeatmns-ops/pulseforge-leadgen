# MAX-P0-005 — Autonomous Orchestration, Outcome Verification and Recovery

**Status:** proposed; planning only.  This document is not approval to implement, deploy, apply a migration, activate a worker/schedule/provider/outbound path, or release a historical hold.

**Authoritative baseline:** `bf4161c7d12e687c4daf8c088c3f5add02de2e01` (`main`, 2026-11-26).  P0-001 through P0-004 are accepted only within their documented scopes.  Anthropic is retired and is not an eligible P0-005 provider, evaluator, fallback, credential, or activation path.

## 1. Decision and scope

P0-005 makes Max responsible for an *authorized objective through a verified terminal outcome*, rather than treating a successful specialist invocation as the outcome.  It must do so with bounded plans, durable dependency progression, independent evidence reads, bounded recovery, and explicit intervention when certainty or authority is insufficient.

It extends, rather than duplicates:

| Existing authority | P0-005 use |
| --- | --- |
| `operator_objectives` | Canonical objective and authorization context; add only objective lifecycle/governance fields needed for orchestration. |
| `max_durable_execution_tasks`, task dependencies and events (P0-004) | The sole durable work graph, checkpoint/event history, leases, dependency gates, cancellation and recovery substrate. Root objective work and child plan steps are durable tasks; no second task store or execution ledger. |
| P0-003 external-action ledger, claims and request receipts | The sole external-effect and idempotency authority. P0-005 verification reads/reconciles it; it does not mint another action ledger. |
| `scripts/max-reliability/recover.cjs` | The one recovery/inspection command. P0-005 adds objective/verification plan and apply operations to it, preserving tenant confirmation and operator attribution. |
| `specialist_delegations`, results and evaluations | Capability/contract/provenance records. They are inputs to a durable task and evidence candidates, not an alternative execution queue or proof of outcome. |
| conversation context/ledger and mission persistence | Continuity hints and mission state only. A resumed objective is rehydrated from the durable root task and revalidated; conversation state never grants authority. |

Out of scope: production deployment; migration application; changing any tenant authorization; enabling `MAX_DURABLE_TASKS`; enabling a worker, cron, provider, outbound execution, or historical hold release; autonomous approval of an external action; effect/ROI optimization; broad remediation of legacy Anthropic endpoints.

## 2. Evidence-backed current architecture and gaps

### Accepted foundation

- P0-001 recorded that synchronous HTTP specialist execution had no durable in-flight ownership, independently read-back verification, bounded retries, or scheduled recovery.  It also distinguished provider acceptance from delivery/effectiveness.
- P0-002's acceptance report requires Anthropic to remain intentionally retired and prohibits restoring its credential or spending policy.
- P0-003 supplies tenant-scoped PostgreSQL leases/CAS, external-action claims, receipts, durable audit/recovery, process-crash testing, and deliberately manual recovery while workers are disabled (`docs/MAX-P0-003-RELIABILITY-CORE.md`).
- P0-004 supplies the default-off durable task graph, DB-time task fence, immutable input/dependencies, one active task per mission, task-linked P0-003 claims/receipts, explicit recovery/cancellation/escalation, and tenant-scoped plan-first recovery.  Its closeout explicitly says P7/background execution is not implemented and a queued task is not automatically picked up (`docs/MAX-P0-004-DURABLE-EXECUTION-OWNERSHIP.md`, criteria 5 and 12, known limits).
- Hosted CI includes `test:max:reliability-core`, while production migrations and activation remain unapplied/unapproved at the accepted baseline.

### Gap register

| Area | Existing capability | Gap P0-005 closes |
| --- | --- | --- |
| Objective decomposition | Workspace objective resolution and operator objectives; P0-004 can parent/depend tasks. | No durable, bounded objective plan whose revisions, terminal criteria, dependency graph, and stop conditions are executable and reviewable. |
| Specialist selection/context | Capability registry, bounded delegation envelope, retrieval/context layers and specialist provenance exist. | Selection is not a policy-ranked, capability/authority/freshness/budget checked admission step, and no canonical task input snapshot is assembled for every child task. |
| Delegation progression | P0-004 enforces task DAG admission/propagation and ownership. | Delegations are not consistently materialized as task-owned children and no driver advances ready work after a request ends. |
| Outcome verification | `ExecutionEvidence.classifyExecutionEvidence` classifies handler-returned records; P0-004 requires evidence before `completed`; specialist evaluation says results are not ground truth. | No independent verifier rereads the ledger, durable records, provider status, or inbound correlation before declaring a task/objective verified. |
| Recovery | P0-003/P0-004 have plan-first operator recovery, fences, claims and escalation. | No bounded autonomous recovery policy or claimant to perform safe progression; no objective-level decision when retries/dependencies/verification cannot resolve. |
| Continuity | Objectives/missions and optional conversation context are durable; recovery rechecks mission references. | No authoritative root-objective checkpoint that restarts planning/verification without relying on a chat session. |
| Governance | Existing monotonic authorization constraints, tenant policy, action ledger and model/API metering exist. | No one admission decision binds authorization, action class, provider allowance, deadline, step/retry/cost caps, and escalation target to the objective/child task. |
| Evaluation | P0-004 has real-PostgreSQL and crash/fence suites. | No behavioral suite proving Max owns a multi-step objective through independently verified success, bounded non-success, and restart recovery. |

## 3. Proposed architecture

### 3.1 Objective contract and bounded plan (package A)

Add an **Objective Orchestration Contract** to an existing `operator_objectives` row and create its P0-004 root task with `execution_scope.kind = objective_orchestration`. The immutable root-task input includes: tenant, objective revision, requested/authorized outcome, success predicates, allowed action classes/specialists/providers, evidence freshness limits, deadline, maximum plan steps, maximum active children, retry and cost ceilings, named escalation policy, durable wake conditions, idle policy, and verification polling/backoff policy. Governance is part of this first package: a plan cannot be admitted without its progressively enforced policy snapshot.

The planner produces a versioned, bounded plan snapshot in the root task checkpoint/result references: deterministic step IDs; prerequisite IDs; selected capability; least authority; minimal context/evidence references; expected deliverable; verifier kind; deadline slice; and a compensation/escalation rule.  A plan is admitted only when it is acyclic, within every ceiling, every capability is callable, and every potentially external step has an existing authorization/action-ledger route.  Planning is advisory until this admission succeeds; it cannot broaden objective constraints.  Replanning is a fenced root-task transition, supersedes only unstarted children, and has a capped revision count.

### 3.2 Context assembly and specialist admission (package B)

Introduce an `ObjectiveTaskAdmission` adapter at the existing P0-004 task creation boundary.  It chooses from `SpecialistCapabilityRegistry` only after: tenant/role and monotonic-constraint checks; required capability/authority check; provider eligibility; evidence provenance/freshness check; objective/step budget check; and conflict/active-mission check.

It writes the compact, redacted, immutable input snapshot to the child task and creates/links the existing specialist delegation record.  The snapshot contains only task-relevant context, canonical IDs/revisions and evidence references—not a workspace session, credentials, unrestricted conversational history, or another tenant's records.  Selection must be explainable as a task event naming candidates rejected and the policy reason.

### 3.3 Durable graph progression (package C)

Root and child work use only P0-004 tasks/dependencies/events.  A child is created before a specialist can run; a child result is reported to `awaiting_verification`, never directly to objective success.  P0-004 dependency propagation remains the authority for blocked descendants.  The root checkpoint records its plan revision, child IDs, verification state, aggregate budget use, and next safe action.

The root reaches `awaiting_verification` only when all required children have independently verified outcomes.  It reaches `completed` only when the objective success predicates are independently satisfied; optional/failed branches are acceptable only if the immutable plan explicitly permits them.  Otherwise it is `blocked_dependency`, `recovery_required`, or `escalated`, with no fabricated "done" presentation.

### 3.4 Independent verification (package D)

Create a `OutcomeVerifierRegistry`, not a second ledger.  Each verifier has a declarative evidence contract and only read/reconcile access appropriate to its type:

- internal mutation: reread the committed mission/objective revision under tenant scope;
- outbound/provider acceptance: reread the P0-003 task-linked claim/receipt and, when authorized, provider status using the stored correlation ID;
- inbound/customer result: use a tenant-scoped persisted webhook/Riley observation correlated to the claim/receipt;
- discovery/research: reread the persisted non-synthetic contribution/provenance and required freshness;
- specialist deliverable: reread the stored delegation result and validate its contract, but do not treat self-reported completion as external outcome proof.

Verifier output is a task event plus evidence references and one of `verified`, `pending`, `contradicted`, `unverifiable`, or `error`.  It must be idempotent, tenant-scoped, time-bounded, and cannot invoke an action provider.  `verify` may transition a task only after this read path succeeds.  A contradiction blocks/escalates according to the contract; an unreadable external effect remains intervention, never retry.

### 3.5 Durable wake, idle and verification policy (package A, completed by E)

The root contract defines the only wake conditions: accepted-objective admission, a prerequisite task terminal event, a task lease/deadline becoming due, an explicitly correlated inbound observation, an explicit operator reverify/recover request, and a due verification check. A wake is persisted as a task event and deduplicated by objective/task/reason/version; it is not an HTTP-session callback.

An idle objective has no runnable child and no due verification. It consumes no model, specialist, provider, or metered API call; its next wake is the earliest persisted deadline/poll time or an allowed inbound/event wake. Verification polling is permitted only while a contract predicate is `pending`, uses a per-predicate deadline and capped attempt count, starts at an approved minimum interval, uses bounded exponential backoff with jitter, and stops permanently on verified, contradicted, uninterpretable/unverifiable, budget exhaustion, or deadline. A poll is a read/reconcile operation only. It must check cached/fresh durable evidence before any provider read and must not trigger a model call.

Package A validates and persists these policies and the governance limits. Package E implements the default-off driver that consumes them; no package may create an implicit polling loop before then.

### 3.6 Execution driver and bounded recovery (package E)

**Finding:** an execution driver is required for autonomous progression. P0-004 deliberately has no worker, scheduler, claimant, or automatic recovery; its HTTP heartbeat is not a driver.  Therefore P0-005 cannot truthfully claim autonomous ownership while work remains queued after request/process termination.

Specify a single `MaxObjectiveDriver` that uses the P0-004 store's claim/fence/heartbeat APIs and `recover.cjs` policy primitives. It is not a new queue. On each bounded tick it: claims only eligible queued children/root continuation tasks; revalidates admission/dependency/freshness/budget immediately before dispatch; invokes the existing routed/adapted specialist under the task fence; schedules an independent verifier; and processes expired leases through the existing recovery logic.  It has one configured concurrency cap, per-tenant fair selection, finite tick/claim/verification timeouts, and an advisory single-flight guard using the P0-003 reviewed dedicated-connection pattern.

Automatic actions are deliberately narrow: retry only a pre-dispatch, classified-transient, no-external-effect task within its immutable retry budget; never retry an unresolved claim, provider ambiguity, authorization/budget failure, dependency drift, deadline failure, verifier contradiction, or unknown exception. Those cases escalate with the exact reason and evidence.  The driver records every decision as a task event and stops on schema, policy, or fence failure.

Implementation may ship this driver code default-off for review and local/disposable-PostgreSQL testing. Worker/schedule activation is a separate production decision after the prerequisites in section 7; this specification does not authorize it.

### 3.7 Governance, budget and provider boundaries (introduced by A; completed by F)

`ObjectiveGovernance` is introduced by Package A and evaluated at root admission, every child admission, immediately before dispatch, and immediately before retry. Its durable policy snapshot is part of the root input and may only tighten thereafter. Package F completes the integrations and verification; it does not introduce governance for the first time. It enforces tenant/actor authorization, capability authority, allowed action class/provider, external-action approval binding, deadlines, maximum steps/children/replans/retries, concurrency, and cost reservation/settlement through existing metering/ledger integrations. Budget exhaustion becomes `escalated: budget_exhausted`; it never silently downgrades to an unmetered call.

Aggregate objective budgets reserve and settle atomically across concurrent child work. Reservation records use the existing durable task/event and metering/ledger transaction boundary, keyed by objective root task and budget dimension; the compare-and-set admits a reservation only if `settled + reserved + requested <= cap`. Settlement releases or converts the exact reservation once, under the child task fence. A process crash leaves the reservation durable; recovery reconciles it from the task/claim/receipt state and either settles, releases only when no external effect is possible, or escalates. No child may make a chargeable/provider call without a current reservation.

P0-005 allowlists only explicitly approved non-Anthropic providers. No code path may read `ANTHROPIC_API_KEY`, instantiate an Anthropic client, select Anthropic as a fallback/default, or reserve Anthropic spend for a P0-005 task/verifier/evaluation. Gemini, deterministic fixtures, or other future providers require their own explicit policy entry and existing credential/meter route.

### 3.8 Persistent objective continuity and operator surface (package G)

Resume derives state from the root task plus tenant-scoped task graph, objective row, P0-003 receipts/claims, and verifier evidence. Conversation context may supply a referent only; it is revalidated and cannot resume an owner token or approval. Add tenant-scoped read-only objective/task status that reports: objective state, current plan revision, ready/blocked/in-flight child counts, verified predicates, budget/deadline remaining, and exact intervention reason. Only independently verified `completed` is presented as achieved.

Extend the existing recovery CLI with `--objective` plan, `--apply`, `--reverify`, and explicit `--resolve-intervention` operations. Mutations retain `--tenant`, matching `--confirm-tenant`, and `--operator`; plan mode writes nothing.

## 4. State transitions and ownership boundaries

| Object | Allowed progression | Owner/boundary |
| --- | --- | --- |
| Objective/root task | `requested → queued → claimed → running → awaiting_verification → completed`; alternatives: `blocked_dependency`, `recovery_required`, `failed`, `cancelled`, `escalated` | P0-004 task CAS/fence remains authoritative. Planner/driver cannot mutate objective state outside a fenced root task. |
| Plan revision | `draft → admitted → executing → superseded | terminal` | Root task only; immutable admitted snapshot; supersede only unstarted work and preserve events. |
| Child/specialist task | P0-004 lifecycle; specialist completion ends at `awaiting_verification` | Driver or request path holds task fence; specialist has only the bounded input and cannot complete/root-success itself. |
| Verification | `pending → verified | contradicted | unverifiable | error` | Verifier rereads independent durable/provider/inbound evidence; task event/result refs retain proof. No outbound authority. |
| External action | Existing P0-003 claim lifecycle | P0-003 is sole effect/idempotency authority; P0-005 only uses task-linked claims and treats unresolved effects as escalation. |
| Recovery | `expired → recovery_required` only where no external effect is possible; otherwise `escalated` | Existing P0-004 recovery/store; driver executes only policy-eligible recovery under fence. Operator resolves all ambiguous effects. |

**Root/child ownership and mission invariant.** A root objective task has `mission_id = NULL`, so it never occupies P0-004's one-active-task-per-mission slot. A mission-bound child carries that mission ID and is the only task permitted to claim it. The root may be active while a child is active because their ownership domains differ; it may aggregate only through its own root fence and may not write a mission. Child task/mission commits retain the existing mission lease and child task fence. Lock order is fixed when both task rows must be read: root task, then child task, then the existing mission lease; no code holds a mission lease while acquiring a root lock. This preserves the P0-004 unique index, prevents parent/child deadlock, and leaves no unlocked or inherited mission-write path.

No component may: create an unowned specialist run; bypass `routeExecutionRequest`/task fencing; write a mission without its existing lease and task fence; infer completion from a model/handler assertion; reuse a stale owner token; expand authority/budget; or re-enable Anthropic.

## 5. Measurable acceptance criteria

1. With P0-005 enabled in a disposable database, every admitted objective has exactly one root P0-004 task and every specialist invocation has one child task/delegation link created before specialist code; structural tests inventory all production entry points.
2. A plan exceeding step, child, dependency, deadline, capability, authority, provider, or budget limits is refused before any specialist/provider call; tests prove zero calls.
3. The persisted plan is a tenant-scoped acyclic graph; concurrent equivalent admission yields one root/logical plan and no duplicate child dispatch. Direct SQL cross-tenant/immutable/cycle attempts fail.
4. Specialist selection records a policy explanation and passes only a callable capability with supported least authority and fresh/relevant bounded context. Full session/secret leakage tests pass.
5. A specialist's success report alone leaves its task/root `awaiting_verification`; only a verifier reread of the prescribed evidence can complete it. A forged handler record cannot achieve `completed`.
6. Provider acceptance, inbound effect, discovery evidence, and contradictory evidence each follow their declared verifier contract; all verifier calls are read-only and idempotent.
7. Process death at every driver boundary (before claim, after claim, during specialist, after specialist/before verification, during verification, and after external dispatch) produces only the documented recover/escalate result, never a duplicate external action or unverified completion.
8. Auto-retry occurs only for a classified pre-dispatch transient failure; it honors the immutable retry/deadline/budget caps. Each forbidden retry class produces a durable escalation and zero retry/provider calls.
9. Restart/resume reconstructs the same root graph/checkpoint from PostgreSQL with a new driver process; a missing/stale conversation context cannot change tenant, authority, plan, or owner fence.
10. `MAX_AUTONOMOUS_OBJECTIVES` and the driver flag are default-off; without both, existing P0-004/default-off behavior remains byte-for-byte compatible. No worker/schedule starts merely by deploying the code.
11. A negative suite proves no P0-005 source/configuration path references an Anthropic credential/client/provider/fallback or Anthropic spend reservation.
12. The complete P0-003/P0-004 reliability lane, relevant Max unit lane, and new behavioral lane pass with no baseline exception or skipped PostgreSQL/process-interruption test.
13. Wake reasons are durable and deduplicated; an idle objective produces zero model/specialist/provider calls. Due verification reads obey its capped backoff and stop conditions, proven with fake time and metered-call counters.
14. Under concurrent children, aggregate budget reservation and settlement are atomic. A killed reservation holder is reconciled from the task/claim/receipt state without overspend, double release, or silent retry.
15. A root objective may remain active with one mission-bound child, but two active tasks for that mission are still rejected. Concurrent aggregation/child commit tests prove the fixed lock order, no deadlock, no fence bypass, and no unlocked mission write.

## 6. Required verification and behavioral evaluations

### Real PostgreSQL/process-interruption requirements

Run against disposable PostgreSQL (not mocks; `initdb`/`pg_ctl` required), with at least two pools and separate OS processes. Include migration re-run and direct SQL negative cases. Inject `SIGKILL`, backend termination, lease expiry, cancellation, driver restart, simultaneous drivers, stale fence, verification timeout, receipt replay, and task dependency drift. Assert durable rows/events/claims/receipts after each boundary and restart from a fresh process/pool.

The suite must extend `npm run test:max:reliability-core`; do not replace the P0-003/P0-004 tests or weaken their exact-once/fence assertions. It must prove: one active mission task remains enforced; task-linked dispatch is fenced; P0-003 unresolved claims escalate; and the driver uses neither a second ledger nor a second task queue.

It must additionally run a real-PostgreSQL concurrent budget-reservation/settlement test (including a crash between provider claim and settlement) and a root/child lock-order test with two pools. A database deadlock, a timeout that leaves an ambiguous lock owner, an overspend, or a second active mission child is a failure.

### Behavioral evaluation scenarios

| Scenario | Required result |
| --- | --- |
| Authorized two-step research objective | Plan creates bounded Scout child then verifier; root completes only after reread non-synthetic persisted evidence. |
| Specialist claims success without stored proof | Child/root remain awaiting verification or escalate; UI never says done. |
| Authorized outbound action returns provider receipt | P0-003 claim/receipt is reread; provider acceptance is reported precisely, not delivery/effectiveness. |
| Ambiguous provider result/crash after dispatch | No retry; root escalates with claim correlation and operator action. |
| Dependency failure/drift | Descendants block transactionally; driver does not dispatch them; root exposes blocker. |
| Budget/deadline/authority exhaustion | Admission/pre-dispatch fails closed; no call and a clear intervention event. |
| Restart mid-specialist and mid-verifier | New process rehydrates from PostgreSQL and follows the bounded recovery policy; no session-derived authority. |
| Concurrent drivers/repeated client objective | One fenced execution/one external effect maximum; loser observes live task state. |
| Tenant boundary and malicious context | Cross-tenant IDs, forged evidence refs, stale owner tokens and context injection are refused without disclosure. |
| Retired-provider regression | Any Anthropic provider/key/fallback attempt is rejected before network/meter call; Gemini fixture/deterministic path remains policy-gated. |
| Operator disconnects after submission | With the only request completed and its session discarded, the default-off test driver receives durable wakes, progresses a dependent child graph, independently verifies the success predicates, and marks the root completed without another HTTP request or human action. |
| Decision quality and stopping | Held-out cases score plan relevance, least-authority specialist choice, context/evidence sufficiency, corrective delegation only when the failure is remediable, and escalation/stop when it is not. Safe execution with a poor plan, unnecessary model/API call, or needless retry fails the evaluation. |

## 7. Production prerequisites, rollout gates and exclusions

Before any production activation, all must be separately approved: P0-003/P0-004 migrations verified by the existing read-only schema report; P0-005 migration/schema report and rollback retention plan; green hosted reliability/behavioral lanes; a reviewed production configuration inventory; approved non-Anthropic provider/meter/budget policy; defined on-call escalation and operator runbook; service-account least privilege; observability/alerts for driver, lease loss, verification latency, budget rejection and interventions; a documented concurrency/capacity limit; and a controlled no-outbound/shadow proof.

Activation is staged and separately authorized: code deployed default-off; read-only status/plan inspection; non-external fixture/shadow driver; narrowly scoped enabled tenant/objective class; then any external action class. Each gate has a rollback that disables flags/driver while retaining tasks, task events, claims, receipts and unresolved intervention evidence. No rollback deletes evidence.

This P0-005 proposal explicitly excludes and does not authorize: production migration application; enabling a process worker, cron, queue consumer, background worker, outbound provider, historical hold release, tenant-policy expansion, automatic approval, data deletion, or Anthropic restoration.

## 8. Implementation packages and review order

1. **A — objective contract/plan schema and governance foundation:** extend existing objective/task structures; persist/validate success predicates, wakes, idle/backoff limits, budget policy and root-task ownership shape; provide read-only plan/status projection. No driver or specialist capability.
2. **B — admission/context/specialist selection:** policy snapshot, bounded context assembler, delegation-to-task binding, structural bypass tests.
3. **C — graph progression:** root/child aggregation and task dependency integration.
4. **D — verifier registry:** independent read adapters and evidence contracts, with no action capability.
5. **E — driver/recovery:** default-off claimant, fair bounded scheduling, recovery CLI extension, process tests.
6. **F — governance completion:** complete budget/provider/action integrations begun in A, including atomic reservation/settlement and retired-Anthropic prohibition tests.
7. **G — behavioral evaluation and release artifacts:** PostgreSQL/process suite, scenario harness, runbook/schema report/activation checklist.

Each package is independently reviewable and merges default-off. Package E must not be approved for activation merely because its code/tests exist.

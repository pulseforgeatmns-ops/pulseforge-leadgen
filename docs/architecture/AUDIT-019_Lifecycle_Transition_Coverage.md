# AUDIT-019 — Lifecycle Transition Coverage

> Historical analysis dated August 22, 2026; diagnostic safety revision October 24, 2026.
> No specific production mission, deployment record or complete event timeline was
> established by this report. Its original conclusion that stale pre-SPEC-137
> state was “proven” and other transition paths were “ruled out” was unsupported.
> The origin of an observed inconsistency remains **unknown**.

Related: [SPEC-136](../specs/SPEC-136_Pending_Operator_Decision_Consistency.md),
SPEC-137 (PR #373), and
[AUDIT-008](../../packages/max/workspace/tests/audit008AmoDiscoveryApprovalExecutionTrace.test.js).

## Historical hypothesis, not an incident diagnosis

The historical audit proposed that advancing `stage` without updating
`pendingOperatorDecision` could leave a mission at `understand` with a pending
decision for `discover`. The relevant source symbols include
`Engine.progress()`, `applyStageTransition()`, `assertMissionStateConsistent()`,
`commitDiscoveryStage()`, and persistence/hydration boundaries.

Reproducing that state shape does not establish the first offending transition,
its date, the deployed code version, or the path that wrote a particular row.
An old mission can receive a new inconsistent mutation. Missing events do not
prove that `Engine.progress()` never ran, and an event after a merge does not
prove that the merged version was deployed. Validation on one write path does
not rule out other writers, imports, restores, direct writes or later mutations.

The original report recorded these **merge references**, not verified deployments:

| Reference | Reported merge time (UTC) | PR |
|---|---|---|
| SPEC-136 consistency validation | 2026-08-22 03:13:37 | #372 |
| SPEC-137 canonical lifecycle mutation | 2026-08-22 12:52:50 | #373 |

## What the diagnostic establishes

The dependency-free, importable
[`LifecycleTransitionAudit`](../../packages/acquisition-mission/audit/LifecycleTransitionAudit.js)
checks only the original four observable patterns:

| Pattern | Observation |
|---|---|
| `stage_pending_stage_mismatch` | Pending stage and mission stage differ |
| `discovery_approval_off_discover` | Discovery approval is advertised outside discover |
| `plan_decision_off_discover` | Plan approval/edit/clarification is advertised outside discover |
| `approved_plan_stale_pending` | An approved or immutable plan still advertises a plan decision |

A match returns `inconsistent_origin_unknown`. No matches returns
`no_known_inconsistency`, **not** a full consistency certificate. Other current
invariants require additional mission/artifact context and are outside this
historical check. The production validator remains authoritative.

Creation/update timestamps are returned as temporal context only. Missing,
invalid or timezone-less timestamps have an unknown temporal relation. The
classifier never assigns stale-state or uncovered-transition causality, does
not mutate its input and does not perform I/O.

## Manual read-only database mode

Running the script without arguments, importing it, or setting ambient
`DATABASE_URL` does not open a connection. The default invocation prints help:

```bash
node scripts/audit019LifecycleTransitionCoverage.js
```

Database access requires separate authorization for the target and all of:

1. Explicit `--database` mode.
2. An explicit `--tenant-id`; `--mission-id` optionally narrows that tenant.
3. `AUDIT019_DATABASE_URL` supplied through an approved secret mechanism,
   preferably for a SELECT-only role. Do not put credentials in shell history.

```bash
node scripts/audit019LifecycleTransitionCoverage.js --database --tenant-id tenant_example --mission-id mission_example
```

No `.env` files or shared application pool are loaded. A dedicated `pg.Client`
uses a read-only repeatable-read transaction, connection/statement/idle timeouts,
parameterized tenant and mission filters, and deterministic rollback/connection
closure on both success and failure. The script executes no repairs, migrations,
agents, providers or application initialization.

Mission results default to 100 rows, configurable with `--limit 1..1000`.
Transition observations are limited to 1000 per matching mission. Both limits
use an extra row to detect truncation and report it explicitly; they are not
exhaustive counts. Events are constrained by both tenant and mission. Only
pattern matches receive event observations; `transitions: null` means events
were not queried. Zero returned events is not proof of non-execution.

Output contains mission IDs/stages, pattern findings, temporal context and
event IDs/timestamps, not raw mission or event payloads. Handle it as scoped
operational data. Errors do not print driver messages or connection strings.
The read-only adapter is covered with offline fakes; this revision does not
claim a live PostgreSQL or production validation.

## Verification and next evidence

Offline regression suite:

```bash
node --test test/audit019LifecycleTransitionCoverage.test.js
```

Tests cover all four patterns, old missions with recent mutations, missing and
invalid timestamps, import/default-mode safety, explicit scopes, parameterized
reads, bounded/truncated results, read-only transaction order, and cleanup on
success and failure. No database/provider credentials or real connections are
needed. These are diagnostic unit/adapter tests, not complete application CI.

To establish origin, an independently authorized investigation would need
before/after state or equivalent mutation evidence tied to the mission, writer,
event sequence and verified deployed version. Until then, neither historical
stale state nor an uncovered transition is proven or ruled out.

Any remediation needs its own scoped review and explicit approval. This report
does not recommend deleting rows, clearing decisions or replaying approval flows.

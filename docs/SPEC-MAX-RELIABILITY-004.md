# SPEC-MAX-RELIABILITY-004 — System-Level Reliability Recovery (P0)

Status: engineering change for review. **Not deployment authorization.**

Max must translate intent into the correct workflow, keep in-flight work,
persist missions durably, and report only verified outcomes.

Lifecycle: `INTENT → CONTEXT RESOLUTION → ROUTING → AUTHORIZATION → EXECUTION → VERIFICATION → RESPONSE`

## 1. Root cause (PR #1063 regression)

PR #1063 made the AMO handler trust `objectiveResolution.primaryObjective ===
mission_creation`. CI went from 1616/1616 (main `b74a2dd`) to 1573/45. The 45
failures were all introduced by that PR, and all had the same cause, which was
already present on `main` but hidden:

1. **Negation-blind classifier.** `OperatorObjectiveResolutionEngine.extractSegmentCandidates`
   and `MessageTypeClassifier.isMissionCreationMessage` matched
   `create … mission` inside *"Do not create a mission."*. Prohibitions were
   classified as the action they forbid.
2. **Precedence inversion.** `WorkspaceOwnershipResolver.resolveWorkspaceOwner`
   returned `MISSION_CREATION` for that objective **before**
   `claimsActiveDeskWorkflow` and before any `activeWorkContext` check.
3. **Accidental rescue on main.** `maybeHandleAcquisitionOwnershipTurn`
   re-parsed the text, declined, `fallbackToReasoning('mission_creation_unhandled')`
   fired, and the *late* `maybeHandleActiveWorkContinuation` (after the owner
   pipeline) happened to handle the turn. Trusting the structured decision
   removed the rescue, so desk-workflow turns became new missions.

The same mechanism produced generic reasoning for real mission requests (the
AUDIT-046 prompt on `main` answers "I can investigate today's briefing…"), and
the review-only incident prompt ("Set up a review-only … mission. No outreach…")
only looked like mission creation because of words *inside* its prohibition.

## 2. Routing precedence contract

`resolveContinuationPrecedence(question, { session })` in
`WorkspaceOwnershipResolver.js` is applied whenever the structured decision is
`mission_creation`. Highest first:

| # | Condition | Owner |
|---|---|---|
| 1 | Explicit new objective: `isExplicitNewMissionRequest`, `isExplicitContextOverride`, or negation-aware mission-creation language | `mission_creation` |
| 2 | Active desk workflow cue (`claimsActiveDeskWorkflow`) | `active_mission` (`active_desk_workflow_precedence`) |
| 3 | Session `activeWorkContext` has entities **and** the *affirmative* text is a follow-up / transform / table-update cue | `active_mission` (`active_work_context_precedence`) |
| 4 | Otherwise | `mission_creation` (structured decision stands) |

The override is recorded as `overriddenObjective: 'mission_creation'`. The rule
reads only the utterance and session state — no tenant branches.

`NegatedDirective.stripNegatedDirectives` removes clauses that start with a
negation (`do not`, `don't`, `never`, `must not`, `without`, `no`) up to the
next clause boundary. Request detectors see only affirmative language; the
operator's prohibitions remain in the original text for constraint parsing.

"Set up a/an/the/new … mission" is recognized as creation language.

Mission-creation turns now return through `traceAskReturn`, so routing trace,
conversational state, classification and objective resolution are recorded.

A routed `mission_creation` that cannot run returns an explicit blocked turn:
`acquisition_mission_tenant_required` or
`acquisition_mission_persistence_unavailable` — never generic reasoning.

## 3. Durable mission state

| Path | Before | After |
|---|---|---|
| `utils/missionRuntime.boot` (SPEC-022 engine), no pool / init failure | silent in-memory store, cached forever | rejects `MISSION_PERSISTENCE_UNAVAILABLE`; failed boot not cached (next call retries) |
| explicit ephemeral | `inMemory: true` | `inMemory: true`, `allowEphemeral: true`, or `MISSION_STORE_ALLOW_EPHEMERAL=1` (labeled `engine.persistence`) |
| AMO `runtime.hydrate` load failure | logged, continued on stale in-memory view | throws `MISSION_PERSISTENCE_UNAVAILABLE` (absent schema still treated as empty) |
| AMO `runtime.create` durable write failure | in-memory mission survived (phantom; later "resumed") | store restored from snapshot; throws `MISSION_PERSISTENCE_UNAVAILABLE` |

`getMissionPersistenceStatus()` reports `{ durable, store, reason, at }`.
Callers (`utils/maxRuntime`, `routes/missions`, `routes/maxWorkspace`) already
catch boot errors; the HTTP surface responds "No review or action is confirmed."

## 4. Execution evidence

`packages/acquisition-mission/ExecutionEvidence.js` classifies every
`routeExecutionRequest` result. States (adapted to existing CER/TME contracts):

`accepted · persisted · dispatched · running · awaiting_authorization ·
completed_verified · already_executed · blocked · failed · outcome_unknown`

| Handler result | State | Reportable as done |
|---|---|---|
| `rolledBack: true` | `failed` | no |
| `alreadyExecuted: true` | `already_executed` | yes (prior commit) |
| `blocked` / `executionOutcome: 'blocked'` | `blocked` | no |
| committed `transactionId` + `executionOutcome` absent or `completed` | `completed_verified` | yes |
| anything else | `outcome_unknown` | no |

The router returns `execution = { state, verified, reason, evidence, correlation }`
and writes `executionState` to its audit row. `correlation` carries request,
mission, tenant, operator, intent, specialist, runtime owner, execution mode,
authorization scope (permissions), parent request and transaction ids.

`AcquisitionMissionExecution.applyExecutionTruth` records the state on
structured metadata. When a work specialist (Scout, Paige, Emmett, Penny, Rex,
…) is not verified, Max says so: *"Scout was dispatched, but completion is not
verified — no committed stage evidence was returned. Treat this as outcome
unknown."* / *"Emmett did not complete this stage: it is blocked."*

Authorization is unchanged and remains independent of conversational output:
`assertPermissions` (`canExecute:false`, missing operator),
`assertExecutionPolicy` (`read_only`, `execution_disabled`),
`assertRuntimeOwnership`, tenant-scoped `engine.get` (`amo_tenant_mismatch`),
SPEC-131 approval consumption in the commit, and handler `alreadyExecuted`
idempotency. Routing confidence never grants authorization.

## 5. Tests

- `packages/max/workspace/tests/p0MaxReliabilityRecovery.test.js` (in `test:max`)
  — the regression matrix: new vs continuing work, ambiguous follow-up,
  explicit new objective, structured-decision conflict, negation, tenant
  independence, review-only import (no CER, no contributions), permission /
  policy block, unsupported intent, cross-tenant routing, verified / unknown /
  failed / blocked evidence, idempotent retry, persistence fail-closed and
  recovery, atomic create rollback, tenant isolation, status read.
- `test/maxReliabilityRecoveryPostgres.test.js` (in
  `test:max:expectations:postgres`) — real disposable PostgreSQL: commit,
  restart recovery, no duplicate on re-issue, tenant isolation, lost database
  fails closed with no phantom mission.

## 6. Known limits

- An operator prohibition such as "No outreach" is enforced by the existing
  approval gates, not yet stored as a machine-checked mission constraint.
- `ExecutionEvidence` classifies structurally; it does not re-read provider
  receipts. A handler that fabricates a `transactionId` would be trusted.
- Generic `fallbackToReasoning` remains for other owner branches
  (specialist, inspection, blueprint…). Only the mission-creation seam was
  converted to explicit blocked states.
- The broad `test:amo` package suite has 67 failures on `main` that pre-date
  this change and are not part of the CI gate.

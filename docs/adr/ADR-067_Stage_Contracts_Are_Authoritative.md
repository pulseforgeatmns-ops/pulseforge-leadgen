# ADR-067 — Stage Contracts Are Authoritative

## Status

Partial safety reconciliation, 2026-10-24. The original PR #403 proposed both a
contract-first human-decision gate and validation of every automatic stage edge.
This reconciliation implements the gate without claiming that the newer
transition graph has been reconciled. Full transition validation remains pending.

## Context

Autonomous progression previously required both a human-decision stage contract
and a successfully constructed `MissionPause` before reporting a pause. At an
Execution/Ready boundary with no presentation metadata, the runner could report
`complete`. That result is not evidence of authorized external execution; current
lifecycle, approval, routing, and transactional guards remain independently required.

## Implemented safety boundary

- `resolveHumanDecisionGate` reads the existing stage contract before constructing
  pause presentation. A required decision returns `shouldPause: true` even if
  `deriveMissionPause` returns `null`. At the logical Execution boundary, the
  existing `hasConsumedExecutionApproval` predicate determines whether a current,
  artifact-bound approval has already satisfied that decision. Invalidated,
  superseded, or revision-mismatched approvals do not satisfy it. Raw approval
  flags are not used. Satisfying the gate does not advance a stage or dispatch sends.
- Missing metadata produces an `ExecutionBlock` with `pauseFallback: true`. It is
  presented as **Mission Paused**, not as successful completion or permission to act.
- The runner checks this boundary both inside its loop and after its step budget.
- `Engine.inspect` exposes the contract, outcome, pause, and block consistently.
  In inspect, an actionable runtime blocker takes precedence over fallback
  presentation. This is not a claim of runner/inspect blocker parity: the runner's
  pre-existing provider-block presentation discrepancy is outside this patch.
- Cancellation stays terminal. Existing runtime errors remain errors. Existing
  pending-decision consistency checks still run before inspection is presented.
- Existing stage definitions, provider/fixture defaults, approval requirements,
  specialist predicates, execution routing, and persistence schemas are unchanged.

## Deferred transition-graph requirement

The original `validateStageTransition(fromStage, toStage)` was an exact `nextStage`
check, with calls in the old plan/discovery branches. It cannot be restored using
obsolete aliases or advertised as validation of today's entire graph:

1. ADR-074 split `discovery` into `discovery_approval` and `discovery_running`.
   The deprecated `DISCOVERY` alias now means approval, whose next stage is running,
   not review. The old discovery-to-review validator call would reject this edge.
2. Plan locking now commits a pending discovery approval. Its current historical
   timeline entry still labels a planning-to-running transition. Execution state,
   contracted edge, and timeline representation must be reconciled deliberately.
3. Max prioritization, acquisition approach, Penny assessment, and Paige preparation
   include legitimate intra-stage work. Exact `nextStage` equality would reject
   these self-transitions. Emmett preparation also crosses PREPARE, which the current
   progression-stage derivation does not explicitly map to acquisition planning.
4. Current `canAutoAdvance*` predicates, lifecycle `canEnter`, transactional
   preconditions, and execution approvals protect prerequisites and authorization.
   They are not an equivalent declarative `nextStage` validator for every automatic
   progression edge. This patch does not remove or replace any of them.

A separate reviewed decision must define stage transitions versus intra-stage
specialist work, reconcile PREPARE and the timeline, then validate all relevant
edges without weakening current guards. The three original validator acceptance
tests are deferred with that work, not silently treated as implemented.

## Verification

The gate tests cover absent metadata at EXECUTE/READY, zero-step exhaustion,
cancellation, no dispatch or state writes, normal discovery-review pauses,
actionable inspect provider blocks, runtime error precedence, current versus
revoked/stale artifact-bound approvals, tenant mismatch, and existing consistency
rejections. Tests use local in-memory fixtures with network access denied.

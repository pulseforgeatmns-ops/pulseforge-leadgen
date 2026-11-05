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
`routeExecutionRequest` result against the **evidence requirement of its
intent**. An internal commit and an external outcome are different things.

| Intent | Requirement | Level |
|---|---|---|
| `EXECUTE_OUTBOUND` | every attempted send record has `status:'sent'` **and** a `providerMessageId` (provider acceptance). Delivery / opens / replies arrive later by webhook and are not implied. | `provider_accepted` |
| `APPROVE_DISCOVERY`, `START_DISCOVERY`, `CONTINUE_INVESTIGATION` | a live (non-synthetic) Scout result committed with a discovery record | `provider_results_committed` |
| everything else (plan, prioritization, Paige variants, Emmett capacity, Penny assessment…) | a committed TME transaction — the artifact is the outcome | `internal_commit` |

| Result | State | Reportable as done |
|---|---|---|
| rolled back by an authorization constraint | `blocked` (`authorization_constraint`) | no |
| rolled back otherwise | `failed` | no |
| `alreadyExecuted` | `already_executed` | yes |
| `blocked` / outcome `blocked` / all sends blocked | `blocked` | no |
| fixture data (`provenance: 'synthetic_fixture'`) | `synthetic_result` | no |
| internal intent committed | `committed_internal` | yes |
| external intent with its provider evidence | `completed_verified` | yes |
| some sends accepted, others failed / unconfirmed | `partially_completed` | no |
| every send rejected | `failed` | no |
| committed, but provider evidence missing or pending | `outcome_unknown` | no |

Lifecycle vocabulary (`accepted · persisted · dispatched · running ·
awaiting_authorization`) names the pre-terminal phases of the existing CER/TME
contracts; they are not stored as a new state machine.

The router returns `execution = { state, verified, reason, evidence,
verification, correlation }`, writes `executionState` to its audit row, and the
REST execute route returns `execution`. `correlation` carries request, mission,
tenant, operator, intent, specialist, runtime owner, execution mode,
authorization scope, parent request and transaction ids.

`AcquisitionMissionExecution.applyExecutionTruth` records the state on
structured metadata and, for any non-verified work specialist, states the
outcome plainly (blocked by constraint, blocked, synthetic, partial, rejected,
or unknown).

**Synthetic data.** `fixtureScoutDiscoveryResult` substitutes fabricated
prospects when live Scout yields nothing or fails. It is now labelled
`provenance: 'synthetic_fixture'`, and fixture fallback is opt-in only
(`allowFixtureFallback === true`) in `runAutonomousProgression`, the AMO runtime
and `POST /api/v1/amo/missions/:id/execute`. Previously autonomous progression
defaulted to `true`.

## 5. Durable authorization constraints

`packages/acquisition-mission/AuthorizationConstraints.js` turns operator
restrictions into `mission.authorizationConstraints = { reviewOnly, prohibited,
sources, origin, establishedAt }`.

| Operator language (in a negated clause unless noted) | Prohibited action classes |
|---|---|
| "review-only", "preparation-only", "read-only" (affirmative) | `provider_research`, `outreach_authorization`, `outreach`, `paid_media` |
| outreach, email, mail, calls, texts, send, launch, execute, print, campaign activation | `outreach`, `outreach_authorization` |
| approve | `outreach_authorization` |
| content, copy, drafts, posts, variants | `content` |
| spend, paid, ads, budget | `paid_media`, `provider_research` |
| discovery, research, prospecting, Scout, Places, enrich | `provider_research` |

Conditional clauses ("do not send without my approval", "don't launch until
approved") are **not** prohibitions; the existing approval gates already
enforce them.

Stage → class: Scout → `provider_research`; Paige → `content`; Penny →
`paid_media`; Emmett `execute` → `outreach`, other Emmett → `outreach_preparation`;
Operator `execution_approval` → `outreach_authorization`; Max → `planning`.

- **Creation:** `createMission` stores constraints from `input.authorizationConstraints`
  and the objective; the Max AMO handoff extracts them from the operator's raw text.
- **Monotonic:** `mergeAuthorizationConstraints` only adds. A resumed request
  with new restrictions tightens and persists; nothing loosens them. Lifting a
  restriction requires an explicit operator decision (not implemented — create
  a new mission).
- **Persistence / recovery:** part of the mission payload; `missionFromRow`
  restores it on hydrate (verified on real PostgreSQL across a restart).
- **Enforcement, independent of conversation:**
  1. `routeExecutionRequest` refuses a prohibited intent before any handler
     runs (`authorization_constraint_blocked`, audited `blocked_by_constraint`).
     Retries and replays hit the same check.
  2. `executeMissionStage` (TME) re-checks every specialist stage, so autonomous
     progression and direct callers are covered; the stage rolls back.
- **Surfaces:** chat renders an explicit blocked turn (approval not consumed);
  REST returns 403.

## 6. Routing fallback integrity

`packages/max/workspace/RoutingFallbackIntegrity.js`. Every
`fallbackToReasoning` is recorded as `{ claimedOwner, reason, executionExpected,
signal }`. Execution is expected when an actionable owner (`active_mission`,
`mission_creation`, `specialist_scout|paige|cal|direction`) declined and the
operator's affirmative, non-interrogative text contains an execution command
(imperative run/launch/send/approve, `isMissionExecutionCommand`, or a Scout
"find … prospects" command). A `mission_execution` structured decision that no
owner claimed (e.g. no mission in the workspace) is treated the same way.

Fallback is deliberately not blocked at the branch: late handlers
(active-work continuation, SPEC-022 mission routes) still own many turns. The
guard sits at the two `composeResponse` sites that produce the generic answer:
an expected-execution fallback becomes an explicit traced **"Not executed"**
turn; informational fallbacks keep the reasoning answer with
`metadata.routingFallbacks`.

| Branch | Fallback reason | Execution expected? |
|---|---|---|
| active mission | `active_mission_unhandled` | if affirmative command |
| mission creation | `mission_creation_unhandled` (now rare — explicit blocked turns) | if affirmative command |
| Scout / Paige / Cal / direction | `specialist_unhandled` | if affirmative command |
| blueprint, mission inspection, knowledge, objective persistence, reflection, identity, conversation layer | `*_unhandled` | no — read/conversational; trail recorded |
| no owner + `mission_execution` decision | `no_mission_for_execution_request` | if affirmative command |

## 7. Tests

- `packages/max/workspace/tests/p0MaxReliabilityRecovery.test.js` (in `test:max`)
  — the regression matrix: new vs continuing work, ambiguous follow-up,
  explicit new objective, structured-decision conflict, negation, tenant
  independence, review-only import (no CER, no contributions), permission /
  policy block, unsupported intent, cross-tenant routing, verified / unknown /
  failed / blocked evidence, idempotent retry, persistence fail-closed and
  recovery, atomic create rollback, tenant isolation, status read.
- `packages/max/workspace/tests/p0MaxReliabilityGaps.test.js` (in `test:max`)
  — constraint extraction / monotonic merge / router pre-dispatch block /
  replay / TME bypass / chat refusal with no Scout call / autonomous
  progression; provider-evidence states; synthetic Scout data; fixture
  default off; fallback detection and the explicit "Not executed" turn.
- `test/maxReliabilityRecoveryPostgres.test.js` (in
  `test:max:expectations:postgres`) — real disposable PostgreSQL: commit,
  restart recovery, no duplicate on re-issue, tenant isolation, lost database
  fails closed with no phantom mission; constraints survive restart, cannot
  loosen, and tighten durably.

## 8. Known limits

- Constraint extraction is lexical. Unusual phrasing ("hold off on any
  emails") may not be recognized; it then falls back to the existing approval
  gates, which still require explicit human approval.
- There is no operator flow to lift a restriction; it is permanent for the
  mission.
- `ExecutionEvidence` trusts handler-supplied fields (`transactionId`,
  `providerMessageId`); it does not re-query Brevo or Places. Provider
  acceptance is not delivery — delivery, opens and replies arrive later by webhook.
- Discovery is verified by provenance + a discovery record; result quality is
  not judged.
- `allowFixtureFallback: true` remains available to authenticated REST callers
  and tests; such results are labelled synthetic but can still be committed.
- Fallback detection is lexical; an execution request with no recognizable
  command language falls back to the reasoning answer (trail recorded).
- The broad `test:amo` package suite has 67 failures on `main` that pre-date
  this change and are not part of the CI gate.

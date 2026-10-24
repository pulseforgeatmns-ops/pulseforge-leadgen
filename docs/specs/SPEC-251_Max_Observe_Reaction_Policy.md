# SPEC-251 — Max OBSERVE Reaction Policy

| Field | Value |
|---|---|
| **Status** | Draft |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-09-14 |
| **Depends on** | [SPEC-118](SPEC-118_Acquisition_Mission_Orchestration.md), SPEC-122 mission inspection (`packages/acquisition-mission/Inspection.js`), AUDIT-072 / AUDIT-073 / AUDIT-074, [SPEC-131](SPEC-131_Transactional_Mission_Execution.md), [SPEC-136](SPEC-136_Pending_Operator_Decision_Consistency.md), [SPEC-169](SPEC-169_Canonical_Mission_Verification.md), [ADR-003](../adr/ADR-003_Human_Approval.md), [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) |
| **Audit** | First divergence: durable canonical observations exist; Max does not consume them as an operator (`docs/audits/AUDIT-OBSERVE_Max_Evidence_Reaction.md` when merged) |

## Objective

While a mission is in `observe`, each new canonical communication observation must cause Max to evaluate what changed and persist internal mission reasoning: candidate disposition, observe evidence tier, next-action recommendation, and follow-up timing.

Max may recommend. Max may not send, call, mutate DNC, or execute follow-up outreach. Autosend remains OFF. Opens are not buying intent.

Success looks like this: after Backus, Meyer & Branch records `sent` + `delivered` + `human_open` with no reply, Max can answer “What changed?”, “What does the open mean?”, “What should we do next?”, and “When should we follow up?” from durable mission state — without anyone asking first to *create* that state.

## Problem

Current OBSERVE path:

```text
provider event
  → canonical provider event
  → durable mission observation
  → taxonomy interpretation
  → optional EXECUTE→OBSERVE / OBSERVE→LEARN
  → STOP
```

Missing:

```text
observation
  → Max evidence assessment
  → candidate belief update
  → mission observe-confidence basis
  → next-action recommendation
  → follow-up timing recommendation
```

Taxonomy already labels `human_open` vs `proxy_open` and correctly refuses to treat opens as business outcomes. Inspection (`explainNext`, `explainConfidenceBasis`) still ignores that evidence. Workspace mode treats `observe` as Complete. There is no durable Max reaction object.

## Scope

- Event-driven Max OBSERVE evaluator on canonical mission observations
- Durable reaction + candidate observe-state contracts
- Deterministic evidence-strength and disposition policy
- Derived follow-up *recommendation* (not execution)
- SPEC-122 inspection of observe reactions
- Workspace: OBSERVE remains an active stage, not Complete
- Idempotent replay of the same observation

## Out of Scope

- Automatic outbound (email, call, SMS)
- Automatic DNC writes
- Legacy `utils/maxWarmthScoring.js` / `utils/maxOrchestration.js` / `prospect_signal_events`
- Redesign of Scout, Paige, Emmett, or Riley
- Changing LEARN eligibility (meaningful business outcomes stay the LEARN gate)
- Numeric warmth / ICP-style scoring of opens
- `pendingOperatorDecision` for follow-up (recommendation only in v1; SPEC-136 stays untouched)
- Mutating Scout `DISCOVERY` / candidate universe rows
- Overwriting Scout `mission.confidence` with an open-derived float

## Design principles

1. **Canonical AMO only.** Observations already persisted on the mission are the evidence. Do not import setter warmth.
2. **Decide upstream of execution.** ADR-016: Max recommends; Emmett/Paige do not invent follow-up from this SPEC.
3. **Human approval remains required** before any later execute of a recommended follow-up. v1 does not execute.
4. **Ordinal evidence, not a new score.** Reuse interpretation types. Add named strength tiers. Do not invent 0–100.
5. **Do not mix Scout confidence with live observe evidence.** Planning confidence stays on `mission.confidence`. Observe evidence is a separate Max-owned assessment.
6. **Cadence is derived, never hardcoded in the evaluator.** If the mission has no sequence-day artifact, timing is `unresolved` rather than a magic “4 days”.
7. **Silent wait is not Complete.** Waiting for a reply or for a recommended follow-up window is still OBSERVE.
8. **Idempotent on observation id.** Duplicate webhook / backfill yields the same reaction.

---

## 1. Proposed canonical flow

```text
canonical observation (already durable)
        │
        ▼
interpretMissionObservation()          // existing taxonomy; unchanged outcomes
        │
        ▼
evaluateObserveReaction()              // NEW — pure, deterministic
        │
        ├─ evidence type + strength
        ├─ prior candidate observe state
        ├─ updated candidate observe state
        ├─ recommended next action + timing
        └─ rationale
        │
        ▼
persistObserveReaction()               // NEW — idempotent on observationId
upsertCandidateObserveState()          // NEW — latest fold per mission+prospect
        │
        ▼
existing lifecycle (unchanged gates)
        ├─ EXECUTE → OBSERVE if queued/launched
        └─ OBSERVE → LEARN only if meaningful business outcome
```

Trigger: `consumeMissionProviderEvent` / `persistProviderCommunicationObservation` after the observation row exists. **Not** operator chat. Riley reply interpretations (`applyRileyReplyInterpretation`) call the same evaluator so `reply_*` / unsubscribe / booking share the contract.

If mission hydration fails, the reaction still persists (same durability class as the observation). Engine memory sync is best-effort, matching #594.

```text
insertBrevoEvent
  → persistCanonicalProviderEvent
  → persistProviderCommunicationObservation
  → persistObserveReaction          // NEW, same process, after observation
  → tryMissionStageSideEffects      // existing hydrate / LEARN / OBSERVE progress
```

---

## 2. Durable data contract

### 2.1 Evidence type

Closed union. Provider taxonomy plus Riley/booking mappings. No new provider event types.

| Evidence type | Source | Notes |
|---|---|---|
| `sent` | observation `eventType=sent` | Transport attempted |
| `delivered` | `delivered` | Transport confirmed |
| `proxy_open` | `opened_proxy` | Weak engagement |
| `human_open` | `opened` | Stronger than proxy; **not** intent |
| `clicked` | `clicked` | Stronger engagement than open |
| `soft_bounce` | `soft_bounce` | Deliverability issue, not terminal by itself |
| `hard_bounce` | `hard_bounce` / `blocked` / `spam` | Terminal negative transport |
| `reply_positive` | Riley `POSITIVE_INTENT` / `WALKTHROUGH_INTENT` | Business signal |
| `reply_neutral` | Riley `NOT_NOW` / `OUT_OF_OFFICE` / `AMBIGUOUS_REPLY` / provider `replied` without Riley | Conversation without clear intent |
| `reply_negative` | Riley `NEGATIVE_INTENT` / `WRONG_PERSON` | Explicit rejection / wrong person |
| `unsubscribe` | provider `unsubscribed` or Riley `UNSUBSCRIBE_INTENT` | Terminal negative |
| `booking` | `BOOKING_CONFIRMED` | Terminal positive |

`deferred` remains taxonomy `transport_deferred` and maps to evidence type `sent` band (transport, not engagement). Do not invent a 13th operator-facing type in v1.

### 2.2 Evidence strength (ordinal, not numeric)

```text
transport_attempted
  < transport_confirmed
  < weak_engagement
  < engagement
  < strong_engagement
  < business_signal
  < terminal_negative | terminal_positive
```

| Evidence type | Strength |
|---|---|
| `sent` | `transport_attempted` |
| `delivered` | `transport_confirmed` |
| `proxy_open` | `weak_engagement` |
| `human_open` | `engagement` |
| `clicked` | `strong_engagement` |
| `soft_bounce` | `transport_attempted` (inbox not confirmed) |
| `hard_bounce` | `terminal_negative` |
| `reply_neutral` | `business_signal` |
| `reply_positive` | `business_signal` |
| `reply_negative` | `terminal_negative` |
| `unsubscribe` | `terminal_negative` |
| `booking` | `terminal_positive` |

Monotonic fold: a candidate’s `evidenceStrength` is the **max** ordinal seen, except `terminal_negative` / `terminal_positive` which lock the candidate.

### 2.3 Candidate disposition (Max-owned)

Do **not** rewrite Scout `candidateUniverse`. This is observe-state only.

```text
unreached → reached → possibly_seen → seen → engaged → conversing
                                                      → interested
                                                      → booked
         ↘ unreachable (hard bounce)
         ↘ rejected (negative / unsubscribe)
         ↘ exhausted (sequence complete, no reply)
         ↘ not_now (Riley not_now; still active, delayed)
```

| Evidence | Disposition |
|---|---|
| `sent` | `reached` if previously `unreached` |
| `delivered` | `reached` |
| `proxy_open` | `possibly_seen` |
| `human_open` | `seen` |
| `clicked` | `engaged` |
| `soft_bounce` | stay `reached` or `unreached`; do not upgrade to seen |
| `hard_bounce` | `unreachable` |
| `reply_neutral` | `conversing` (`not_now` if Riley `NOT_NOW`) |
| `reply_positive` | `interested` |
| `reply_negative` | `rejected` |
| `unsubscribe` | `rejected` |
| `booking` | `booked` |

Active dispositions: `unreached`, `reached`, `possibly_seen`, `seen`, `engaged`, `conversing`, `interested`, `not_now`.  
Terminal dispositions: `booked`, `rejected`, `unreachable`, `exhausted`.

### 2.4 Observe reaction row (append-only)

Table: `acquisition_mission_observe_reactions`

| Field | Type | Meaning |
|---|---|---|
| `id` | text pk | `obsrx_<observationId>` (idempotency key) |
| `observation_id` | text unique | Canonical observation id |
| `mission_id` | text | Mission |
| `tenant_id` | text | Tenant |
| `prospect_id` | text null | Candidate / CRM prospect |
| `evidence_type` | text | Closed union above |
| `evidence_strength` | text | Ordinal tier |
| `interpretation_type` | text | Existing `INTERPRETATION_TYPES` value |
| `prior_disposition` | text null | Before this observation |
| `updated_disposition` | text | After this observation |
| `mission_evidence_tier` | text | Folded mission tier after this row |
| `recommended_next_action` | text | Closed union below |
| `recommended_timing` | jsonb | See §5 |
| `rationale` | text | Operator-facing why |
| `human_approval_required` | boolean | Always true if action is `propose_follow_up` |
| `external_action_permitted` | boolean | **Always false in v1** |
| `cadence_source` | text | `prepared_sequence` \| `unresolved` |
| `payload` | jsonb | Full reaction snapshot |
| `at` | timestamptz | Evaluation time |

Idempotency: `ON CONFLICT (observation_id) DO NOTHING` (or return existing row). Re-ingesting the same observation must not change disposition or timing.

### 2.5 Candidate observe state (latest fold)

Table: `acquisition_mission_candidate_observe_state`

Primary key: `(mission_id, prospect_id)`.

| Field | Meaning |
|---|---|
| `disposition` | Latest |
| `evidence_strength` | Max ordinal (with terminal lock) |
| `last_observation_id` | |
| `last_evidence_type` | |
| `last_reaction_id` | |
| `recommended_next_action` | From latest reaction after fold |
| `recommended_timing` | |
| `sequence_step_sent` | If known from execution record |
| `updated_at` | |

### 2.6 Mission observe assessment (derived, also snapshotted on mission payload)

Not a replacement for `mission.confidence`.

```js
observeAssessment: {
  spec: 'SPEC-251',
  evidenceTier: 'engagement',          // max among active candidates
  planningConfidence: 0.xx,            // existing mission.confidence (Scout)
  confidenceBasis: 'human_open on prospect …; not buying intent',
  recommendedNextAction: 'wait',
  recommendedTiming: { … },
  activeCandidateCount: 1,
  terminalCandidateCount: 0,
}
```

`inspect()` includes `observeAssessment` and `observeReactions`. SPEC-169 projection **adds** `observeReactions` (and candidate observe state) so persistence verification matches memory. Interpretations remain reconstructible from observations; they do not need a new table for v1.

### 2.7 Recommended next action

| Action | Meaning | External action permitted |
|---|---|---|
| `wait` | Remain OBSERVE; no follow-up due yet | false |
| `propose_follow_up` | Human-approved follow-up is appropriate after timing | false |
| `review_reply` | Semantic reply needs operator attention (ambiguous) | false |
| `propose_end_candidate` | Exhausted / unreachable / rejected | false |
| `none` | Lifecycle/business outcome owns the next stage | false |

---

## 3. Files / functions to add or change

| Change | File | What |
|---|---|---|
| **Add** | `packages/acquisition-mission/ObserveReaction.js` | Types, strength ordinal, disposition fold, `createObserveReaction` |
| **Add** | `packages/acquisition-mission/ObserveEvaluator.js` | `evaluateObserveReaction({ observation, interpretation, priorState, cadence, outcomes })` |
| **Add** | `packages/acquisition-mission/ObserveCadence.js` | `resolveObserveCadence(mission, store)` — derived timing only |
| **Change** | `packages/acquisition-mission/Engine.js` | After `applyCommunicationObservationInterpretation` / Riley / booking: `applyObserveReaction`. `inspect()` exposes assessment. |
| **Change** | `packages/acquisition-mission/Store.js` | `addObserveReaction`, `listObserveReactions`, `putCandidateObserveState` |
| **Change** | `services/acquisitionMissionPersistence.js` | Persist reactions + candidate state; include in `persistStageCommit` / hydrate |
| **Change** | `services/acquisitionMissionProviderObservation.js` | Persist reaction immediately after durable observation (not chat; not only after hydrate) |
| **Change** | `services/acquisitionMissionRileyInterpretation.js` | Same evaluator after Riley apply |
| **Change** | `packages/acquisition-mission/CanonicalMissionProjection.js` | Add `observeReactions` (and candidate observe state) |
| **Change** | `packages/acquisition-mission/Inspection.js` | OBSERVE-aware `explainNext`, `explainConfidenceBasis`, `explainTimeline`; classify “what does the open mean” / “when should we follow up” |
| **Change** | `packages/max/workspace/ConversationLayer.js` | `extractMissionFacts` may read `observeAssessment` (presentation only) |
| **Change** | `packages/acquisition-mission/WorkspaceMode.js` | Remove `observe` from `TERMINAL_STAGES`; add active `observing` workspace mode |
| **Change** | `packages/acquisition-mission/index.js` | Export new modules |
| **Do not change** | Scout / Paige / Emmett / Riley classifiers | Mapping only |
| **Do not use** | `utils/maxWarmthScoring.js` | Forbidden |

---

## 4. Interaction with existing belief / confidence primitives

| Primitive | Role after this SPEC |
|---|---|
| `mission.confidence` | **Unchanged source:** Scout discovery / planning (`Engine.contribute`). OBSERVE does not overwrite it with open math. |
| SEC `normalizeConfidence` `{ overall, evidence, fit, completeness }` | Discovery/specialist results only. Not used to score opens. |
| Scout `candidateUniverse[].confidence` | Immutable specialist evidence. Observe evaluator does not patch discovery contributions. |
| `interpretMissionObservation` / `INTERPRETATION_TYPES` | **Input** to the evaluator. Opens stay evidence-only outcomes (`recommendedOutcome: null`). |
| `hasMeaningfulBusinessOutcome` / LEARN | **Unchanged.** Bounce, unsubscribe, Riley intent, booking still create outcomes and may LEARN. |
| `explainConfidenceBasis` | **In OBSERVE:** report `observeAssessment.evidenceTier` + planning confidence separately. Stop saying “No campaign results yet” when observations exist. |
| Legacy warmth (`email_human_opened` weights 5/8/12) | **Out of bounds.** |

Belief, for this SPEC, **is candidate disposition + evidence strength**, not a new float.

---

## 5. Follow-up timing source

Inspected existing primitives:

| Source | Verdict |
|---|---|
| AMO `communicationPolicy` | Conversation style only (`reasoning`, `modifiers`). **No cadence.** |
| AMO `evaluationPolicy` | `executiveBehavior` only. |
| Paige `VARIANTS` contribution | No canonical `day` field on current AMO variant payloads. |
| Anchor `ANCHOR_DRAFT_SEQUENCES` days `[0, 4, 8, 13]` | Client-10 **legacy templates**, not AMO mission state. Must not be hardcoded into the evaluator. |
| Riley `not_now` “30 days” | Inbound note, not OBSERVE cadence. |
| `maxWarmthScoring` windows | Forbidden. |

**Resolver order** (`ObserveCadence.resolveObserveCadence`):

1. Prepared outreach artifact / execution record payload: `steps[].day` or equivalent sequence offsets bound to this `preparedArtifactRevision`.
2. Paige contribution payload if it later carries step days (consume if present; do not require Paige redesign).
3. Else `cadence_source = 'unresolved'`.

When resolved, `N` is the **delta in days from the current sent step to the next unsent step** (e.g. step 0 → step 4 ⇒ 4 days). Clock:

| Situation | Clock start |
|---|---|
| sent / delivered, unopened | last `sent`/`delivered` `occurredAt` |
| proxy_open / human_open / clicked, no reply | that engagement `occurredAt` |
| unresolved cadence | `dueAt: null`, action stays `wait` with rationale that cadence is unspecified |

`recommended_timing` shape:

```js
{
  kind: 'wait_until' | 'due' | 'unresolved' | 'none',
  dueAt: 'ISO-8601' | null,
  waitDays: number | null,       // derived delta, not a global constant
  cadenceSource: 'prepared_sequence' | 'unresolved',
  clockStart: 'ISO-8601' | null,
  businessDays: false            // v1 uses sequence calendar days as stored on the artifact
}
```

v1 does not invent a PulseForge-wide “N business days” constant. If Backus’s prepared revision has a next step at day 4, N=4 from that artifact. If it does not, Max says the wait is appropriate but due-at is unknown — still not Complete, still not a send.

---

## 6. Lifecycle behavior

### OBSERVE stays active while waiting

`observe` is **not** a terminal workspace stage. Waiting for a reply or for a recommended follow-up window is Observing.

`WorkspaceMode.js` today:

```js
const TERMINAL_STAGES = [STAGES.OBSERVE, STAGES.LEARN, STAGES.IMPROVE];
```

Change: `observe` is removed from that list. Add `WORKSPACE_MODES.OBSERVING` (inspect-like components: timeline, health, why, workspace). `learn` / `improve` may remain complete-like.

Do **not** create `pendingOperatorDecision` for follow-up in v1 (recommendation only). SPEC-136 stays consistent.

### What causes which transition

| Condition | Stage | Candidate | Notes |
|---|---|---|---|
| sent / delivered / proxy_open / human_open / clicked / soft_bounce / provider replied without Riley | **remain OBSERVE** | active | Follow-up may be `wait` or `propose_follow_up` |
| Cadence elapsed, still no reply, sequence step remaining | **remain OBSERVE** | active | `propose_follow_up`; no send |
| Cadence unresolved | **remain OBSERVE** | active | `wait`, `timing.kind=unresolved` |
| Sequence steps exhausted, no reply | **remain OBSERVE** | `exhausted` | `propose_end_candidate`; mission not auto-failed |
| `reply_positive` / `booking` | LEARN if existing outcome policy fires | `interested` / `booked` | Business outcome; **mission success** path is booking / walkthrough_booked (existing LEARN→IMPROVE) |
| `reply_neutral` (ambiguous / OOO) | remain OBSERVE | `conversing` | `review_reply` if ambiguous; `wait` if OOO |
| `reply_neutral` (`not_now`) | remain OBSERVE | `not_now` | Wait; do not LEARN unless existing outcome `not_now` is already treated as meaningful — **keep current LEARN gate** (`not_now` is a meaningful business outcome today and **will LEARN**). Do not change that in this SPEC. Observe reaction still records disposition. |
| `reply_negative` / `unsubscribe` / `hard_bounce` | LEARN if existing outcome policy fires | `rejected` / `unreachable` | **Candidate failure**, not automatic mission failure |
| All candidates terminal (`rejected` \| `unreachable` \| `exhausted`) and no booked/interested | remain OBSERVE until LEARN from existing outcomes **or** operator ends mission | — | v1 does **not** add a new `mission_failed` stage. Inspection reports mission-level `propose_end_candidate` when every candidate is terminal without success. |

`not_now` already auto-LEARNs via `isMeaningfulBusinessOutcome`. This SPEC does not reopen AUDIT-074. Observe reaction records `not_now` disposition even if the mission advances.

### Mission success / failure

- **Success:** existing terminal positive outcomes (`walkthrough_booked`, `meeting_booked`, `booking` evidence). LEARN then IMPROVE with meaningful learning, unchanged.
- **Candidate failure:** `rejected`, `unreachable`, `exhausted`.
- **Mission failure:** not a new stage. Operator-visible when no active candidates remain and none succeeded.

---

## 7. Follow-up recommendation policy

Deterministic. Always `external_action_permitted: false`. `human_approval_required: true` iff `propose_follow_up`.

| Situation | Disposition | Next action | Timing |
|---|---|---|---|
| sent, unopened | `reached` | `wait` | Clock from sent; due at next sequence step if known |
| delivered, unopened | `reached` | `wait` | Clock from delivered (or sent if no delivered) |
| proxy opened, no reply | `possibly_seen` | `wait` until cadence; then `propose_follow_up` | Weak evidence; do not treat as intent; still may follow up after cadence |
| human opened, no reply | `seen` | `wait` until cadence; then `propose_follow_up` | Stronger than delivery/proxy; **not** interest |
| clicked, no reply | `engaged` | same as human open | Stronger engagement |
| positive reply | `interested` | `none` (LEARN owns lifecycle) | `kind: none` |
| neutral / question / OOO / ambiguous | `conversing` | `review_reply` if ambiguous; `wait` if OOO | no follow-up send |
| `not_now` | `not_now` | `none` or `wait` depending on LEARN | Riley 30-day note is not this evaluator’s cadence |
| negative reply | `rejected` | `propose_end_candidate` | none |
| hard bounce | `unreachable` | `propose_end_candidate` | none |
| unsubscribe | `rejected` | `propose_end_candidate` | none |
| booking | `booked` | `none` | none |
| soft bounce | `reached` or prior | `wait` | do not propose follow-up email to a bouncing address |

Proxy vs human: both can eventually `propose_follow_up` after cadence; **rationale and evidenceStrength differ**. Inspection must say proxy is weak / possible prefetch; human open is real engagement still without intent.

---

## 8. Explainability (SPEC-122)

Inspection must resolve from durable reaction state, not LLM invention.

| Operator question | Property | Source |
|---|---|---|
| What changed? | `timeline` | Latest observation + reaction rationale |
| What does the open mean? | `observe_meaning` (new classify) or `recommendation` | `human_open` → engagement, not intent; strength vs delivered / proxy |
| What should we do next? | `next` | `recommended_next_action` + approval/external-action flags |
| When should we follow up? | `next` / `waiting` | `recommended_timing` |
| Why this confidence? | `confidence` | planningConfidence + evidenceTier; not “no campaign results” |
| Why isn’t this sending? | `next` | `external_action_permitted: false`, autosend off, human approval required |

`explainNext` today falls through to “Continue in mission workspace.” for OBSERVE. Replace that with the latest `observeAssessment`.

---

## 9. Backus example — before / after

Mission: `mission_ad7753b0-6def-441d-bb1a-3764656f5750`  
Candidate: Backus, Meyer & Branch (`prospectId` bound on execution + observations)  
Evidence: `sent`, `delivered`, `human_open`, no reply.

### Before

| Surface | Value |
|---|---|
| Stage | `observe` / Observing |
| Observations | durable `sent` + `opened` |
| Interpretation | `human_open`, no outcome |
| `mission.confidence` | Scout/planning |
| Inspection next | “Continue in mission workspace.” |
| Confidence basis | “No campaign results yet” |
| Workspace mode | Complete |
| Follow-up | none |

### After (this SPEC)

| Surface | Value |
|---|---|
| Stage | **remain `observe`** (active, not Complete) |
| Reaction on open | `evidence_type=human_open`, `evidence_strength=engagement` |
| Prior disposition (after delivered) | `reached` |
| Updated disposition | `seen` |
| Meaning | Outreach reached the recipient; human engagement is stronger than delivery; **not** buying intent |
| Next action | `wait` if cadence not elapsed; `propose_follow_up` if cadence elapsed |
| Timing | Derived from prepared sequence next-step delta if present; else `unresolved` (still wait, still not send) |
| Human approval required | true only when action is `propose_follow_up` |
| External action permitted | **false** |
| Inspection | Can answer the four operator questions from the reaction row |

---

## 10. Tests

| File | Cases |
|---|---|
| `packages/acquisition-mission/tests/spec251ObserveReaction.test.js` | Evidence matrix; human vs proxy strength; open ≠ intent; disposition fold; idempotent `observationId`; `external_action_permitted === false`; no DNC mutation; no send |
| Backus fixture | sent → delivered → human_open, no reply: disposition `seen`, next `wait` or `propose_follow_up`, rationale contains engagement-not-intent |
| Cadence | prepared sequence day 0 then 4 → `waitDays=4`; missing cadence → `unresolved`, not a hardcoded N |
| LEARN regression | `interested` / `hard_bounce` / `unsubscribe` still LEARN; opens do not |
| Inspection | “what happens next?”, “what does the open mean?”, “when should we follow up?” resolve from reaction |
| Workspace | `deriveWorkspaceMode` for `observe` is **not** Complete |
| Persistence | duplicate consumeMissionProviderEvent does not duplicate reaction; hydrate restores assessment |
| Forbidden | no import of `maxWarmthScoring` |

Keep existing `test/observationInterpretation.test.js` and `specObserveLearnProgression.test.js` green.

---

## 11. First implementation slice

Smallest close of the audit gap. No send. No Paige/Scout/Emmett work.

1. `ObserveReaction` + `ObserveEvaluator` + `ObserveCadence` (pure).
2. Persist reaction + candidate state keyed by observation id.
3. Hook after durable observation write and after Riley/booking apply.
4. `inspect()` + SPEC-122 next/confidence/timeline/open-meaning.
5. WorkspaceMode: OBSERVE is active Observing.
6. Tests in §10 for Backus human open + idempotency + no external action.

Defer: `pendingOperatorDecision` for follow-up approval, executing a recommended follow-up, business-day calendars, mission-failure stage, numeric confidence overlays, interpretation table persistence.

## Migration

- Additive tables only.
- Backfill: for each existing `acquisition_mission_observations` communication row on OBSERVE missions, run evaluator in observation order (same as provider-event backfill). No resend.
- Rollback: drop the two new tables; evaluator hook is skippable if tables missing (fail-open to today’s “observation only” behavior is acceptable for rollback, not for forward).

## Acceptance criteria

- [ ] New canonical observation on an OBSERVE (or just-progressed) mission produces exactly one observe reaction.
- [ ] Duplicate observation ingestion returns the existing reaction.
- [ ] Human open updates candidate to `seen`, does not create a business outcome, does not send.
- [ ] Proxy open is weaker than human open and is not intent.
- [ ] Click is stronger than open and is not intent.
- [ ] Hard bounce / unsubscribe / negative reply set terminal negative disposition; evaluator does not write DNC.
- [ ] `external_action_permitted` is always false.
- [ ] Follow-up timing is derived or explicitly `unresolved` — never a magic number in the evaluator.
- [ ] OBSERVE workspace mode is not Complete merely because the mission is waiting.
- [ ] Max inspection answers the four Backus questions from durable state.
- [ ] LEARN gate unchanged for opens (stay OBSERVE) and for existing meaningful outcomes.

## Future work

- Operator decision card to approve a recommended follow-up (then existing Execute path).
- Persist interpretations as first-class rows if inspect needs them without recompute.
- `communicationPolicy.observeCadence` if missions should declare cadence without a sequence artifact.
- Exhaustion → operator “end mission” decision.
- Business-day vs calendar-day once a canonical calendar primitive exists.

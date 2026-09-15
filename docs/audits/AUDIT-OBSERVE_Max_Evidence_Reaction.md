# AUDIT — Max OBSERVE reaction policy for mission evidence

| Field | Value |
|---|---|
| **Status** | First divergence identified — no redesign |
| **Date** | 2026-09-14 |
| **Mission** | `mission_ad7753b0-6def-441d-bb1a-3764656f5750` (Anchor / Backus, Meyer & Branch) |
| **Current stage** | `observe` / Observing |
| **Constraints honored** | autosend OFF; no automatic external action; no Scout/Paige/Emmett redesign; canonical evidence preserved |
| **Related** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md), SPEC-122 mission inspection (`packages/acquisition-mission/Inspection.js`), AUDIT-072 (communication observation), AUDIT-073 (interpretation), AUDIT-074 (OBSERVE→LEARN), #594/#595 (durable observation persistence) |

## STOP CONDITION

**First architectural divergence:** after canonical observations are durably recorded, **nothing causes Max to inspect them as an acquisition operator.**

| Field | Value |
|---|---|
| **Expected** | New mission evidence would cause Max to inspect, update beliefs/confidence, and recommend wait / follow-up / escalate — without sending. |
| **Actual** | The Brevo webhook records a communication observation and may run a Max-*attributed taxonomy label*. That path does not invoke Max inspection, does not update mission confidence or candidate belief, and does not produce a next-action or follow-up recommendation. |
| **Where the chain breaks** | Between durable observation write and operator cognition: `consumeMissionProviderEvent` → `tryMissionStageSideEffects` → `Engine.recordCommunicationObservation` / `applyCommunicationObservationInterpretation`. No subsequent call to `engine.inspect()`, `answerOperator()`, or any OBSERVE reaction policy. |
| **Files** | `services/acquisitionMissionProviderObservation.js` (`consumeMissionProviderEvent`, `tryMissionStageSideEffects`); `packages/acquisition-mission/Engine.js` (`applyCommunicationObservationInterpretation`); `packages/acquisition-mission/ObservationInterpretation.js` (`interpretMissionObservation` returns `recommendedOutcome: null` for opens); `packages/acquisition-mission/Inspection.js` (`explainNext`, `explainConfidenceBasis`) |

Do not treat this as a Scout/Paige/Emmett gap. Canonical observations for the Backus send and human open can already exist. Max does not consume them as operator state.

---

## Observed request / evidence path

```text
Brevo webhook
  → utils/brevoEvents.insertBrevoEvent
  → persistCanonicalProviderEvent
       (acquisition_mission_provider_events, correlated to outbound execution)
  → consumeMissionProviderEvent
       1. persistProviderCommunicationObservation   ← durable write (post #594)
            acquisition_mission_observations  id = obs_<provider_event_row_id>
       2. tryMissionStageSideEffects                ← best-effort, lock-sensitive
            hydrate mission
            Engine.recordCommunicationObservation
              → interpretMissionObservation          ← taxonomy only
              → applyInterpretationResult            ← outcome only if policy allows
            maybe progress EXECUTE → OBSERVE
            maybe progress OBSERVE → LEARN
            persistMissionState
```

Max conversational inspection is a **separate, operator-triggered** path:

```text
Operator asks
  → packages/max/workspace/WorkspaceMissionInspection.inspectActiveMission
  → Engine.inspect / inspectQuestion (SPEC-122)
  → explainNext / explainConfidenceBasis / explainTimeline
```

That path is not invoked when a new observation arrives.

---

## Answers

### 1. What code path causes Max to inspect new observations?

**None.** There is no Max inspection trigger on new observations.

What *does* run:

- Event-driven recording: `insertBrevoEvent` → `consumeMissionProviderEvent` → `persistProviderCommunicationObservation`.
- Optional in-process taxonomy: `Engine.applyCommunicationObservationInterpretation`, which writes an in-memory interpretation and a timeline event labeled `Interpreted: <type>` with specialist `max`.
- Operator-triggered SPEC-122 inspection only when someone asks Max a mission-state question. That inspection does not read observation `eventType` / interpretation type to decide anything.

`maxAgent.js` daily briefing does not inspect acquisition-mission observations.

### 2. Is OBSERVE event-driven, polled, operator-triggered, or only evaluated when asked?

| Concern | Mode |
|---|---|
| Recording observations | **Event-driven** (Brevo webhook). Backfill exists for provider events that predate the consumer (`scripts/backfillMissionProviderObservations.js`, `auditAnchorOutboundEvidence.js --repair`). |
| Taxonomy interpretation | **Event-driven side-effect** of the same webhook, if mission hydration succeeds. Not durable as its own table. |
| EXECUTE → OBSERVE | **Event-driven**, when a communication observation lands on an eligible EXECUTE mission (`queuedOrLaunched` + `executionSummary.complete !== false`). |
| OBSERVE → LEARN | **Event-driven**, only when a *meaningful business outcome* is recorded (not an open). |
| Max operator reasoning / next action | **Only when asked**, and even then OBSERVE evidence is not in the derivation. No poll. No cron. |

### 3. What evidence types are currently understood?

Canonical taxonomy lives in `CommunicationObservation.js` + `ObservationInterpretation.js`.

| Evidence | Observation eventType | Interpretation | Automatic business outcome? |
|---|---|---|---|
| sent | `sent` | `transport_success` | No (evidence-only) |
| delivered | `delivered` | `transport_success` | No |
| human open | `opened` | `human_open` | No |
| proxy open | `opened_proxy` | `proxy_open` | No |
| click | `clicked` | `link_engagement` | No |
| deferred | `deferred` | `transport_deferred` | No |
| soft bounce | `soft_bounce` | `transport_failure` | No |
| hard bounce / blocked / spam | `hard_bounce` / `blocked` / `spam` | `transport_failure` | Yes → `bounce` |
| provider `replied` (no body) | `replied` | `reply_received` | No — waits for Riley |
| explicit rejection | *not a provider type* | Riley `negative` → `negative_intent` | Yes → `not_interested` |
| unsubscribe | `unsubscribed` | `unsubscribe_intent` | Yes → `unsubscribe` |

Opens, clicks, sent, and delivered are explicitly **evidence-only**. They must not become business outcomes. That constraint is already implemented and tested.

### 4. Does Max distinguish human open vs proxy open?

**At taxonomy, yes. At operator reaction, no.**

- Provider maps Brevo `loaded_by_proxy` / `proxyOpen` → `opened_proxy`; ordinary opens → `opened`.
- `interpretEngagementObservation` maps `opened` → `HUMAN_OPEN` and `opened_proxy` → `PROXY_OPEN`.
- Interpretation does **not** read `payload.openSource`. A later `email_events.open_source` reclassification to proxy would not change an already-written `opened` / `human_open` observation.
- Both types have `confidence: 1` and `recommendedOutcome: null`. Neither updates mission confidence, candidate belief, next action, or follow-up timing.

A **separate** prospect-level warmth path (`utils/maxWarmthScoring.js`, `utils/maxOrchestration.js`, `prospect_signal_events`) does distinguish human vs proxy opens. That path is not the canonical acquisition-mission OBSERVE loop and must not be mistaken for Max mission reaction.

### 5. Does an open affect mission confidence, candidate belief, next action, or follow-up timing?

| Surface | Effect of a human open |
|---|---|
| Mission `confidence` | **No.** `mission.confidence` is set from Scout discovery contribution (`Engine.contribute`). `explainConfidenceBasis` still says “No campaign results yet” unless `snapshot.outcomes` has rows. Opens are not outcomes. |
| Candidate / company belief | **No.** Scout `candidateUniverse` / discovery payload is not updated. Observation carries `prospectId` but does not mutate candidate records. |
| Next action | **No.** `explainNext` has no OBSERVE branch. For a healthy Observing mission it returns `"Continue in mission workspace."` |
| Follow-up timing | **No.** No wait window, no due-at, no pending operator decision in OBSERVE (`derivePendingOperatorDecisionForStage` only handles `discover` and `ready`). |

Correctly, an open is **not** treated as positive buying intent.

### 6. What is the current follow-up policy after each evidence class?

There is **no mission OBSERVE follow-up policy**. Behavior is stay-in-Observing unless a meaningful business outcome exists.

| Evidence | Current behavior |
|---|---|
| Sent but unopened | Observation + `transport_success`. Stay `observe`. Wait indefinitely. |
| Human opened, no reply | Observation + `human_open`. Stay `observe`. No follow-up recommendation, no timer. |
| Replied positively | Riley `interested` / walkthrough intent → `interested` or `walkthrough_requested` → auto OBSERVE → LEARN. |
| Replied negatively | Riley `negative` → `not_interested` → auto OBSERVE → LEARN. |
| Hard bounce / blocked / spam | `bounce` outcome → auto OBSERVE → LEARN. |
| Soft bounce | `transport_failure` only. Stay `observe`. |
| Provider `replied` without Riley | `reply_received`. Stay `observe`. |
| Unsubscribe | `unsubscribe` → LEARN. |

LEARN is a lifecycle advance from *business outcome existence*, not an operator follow-up plan.

### 7. Can Max currently recommend a human follow-up without automatically sending?

**Not from OBSERVE evidence.**

- Interpretation never recommends follow-up.
- Inspection recommendation is Scout cognitive trace / Max prioritization leftovers, not live opens.
- `pendingOperatorDecision` is not created in OBSERVE.
- Max policy (`packages/max/policy`) can require approval for `follow_up_outreach` **if** a recommendation exists. Nothing in the OBSERVE path produces that recommendation.
- Autosend remains off; this audit does not change that. The gap is recommendation, not sending.

### 8. Does mission state advance from observe, and under what conditions?

| Transition | Condition |
|---|---|
| EXECUTE → OBSERVE | Communication observation + queued/launched evidence + execution summary complete. Actor recorded as Max. |
| OBSERVE → LEARN | `hasMeaningfulBusinessOutcome` (Riley-classified reply, bounce, unsubscribe, booking — **not** open/sent/click). Actor Max. |
| OBSERVE otherwise | Stays Observing. |

Presentation mismatch (downstream of this audit, not the first break): `WorkspaceMode.js` treats `observe` / `learn` / `improve` as `TERMINAL_STAGES` and maps them to workspace mode **Complete**. The operator UI therefore treats an active Observing mission as finished work.

### 9. Is evidence tied back to the correct candidate/company throughout?

**Transport/identity binding: yes. Belief binding: no.**

Observations carry:

- `missionId`
- `prospectId` (from canonical outbound execution)
- `evidence.executionRecordId`
- `evidence.providerMessageId`
- `evidence.preparedArtifactRevision`
- `evidence.missionProviderEventId`

Backus regression (`test/anchorBackusProviderObservation.test.js`) asserts those fields for sent and opened rows.

What is not tied:

- Scout candidate universe / company belief objects are not updated.
- Interpretations are in-memory only (`Store.listInterpretations`); they are not a persisted canonical projection key (`CanonicalMissionProjection` has observations, not interpretations). After hydrate, Max can still see the **observation** (`eventType`, `prospectId`) from `acquisition_mission_observations.payload`.

### 10. First architectural gap preventing Max from behaving like a true acquisition operator after this evidence?

**Max has no OBSERVE reaction policy.** Evidence can land. Max does not inspect it, does not update beliefs, and does not recommend wait vs follow-up vs escalate.

That is the first break on the requested trace:

```text
acquisition_mission_observations          ← works (durable, Backus-bound)
  → Max evidence ingestion                ← taxonomy label only; Max-operator never inspects
  → belief / confidence update            ← DOES NOT RUN   ← FIRST DIVERGENCE
  → mission inspection / state            ← OBSERVE evidence not in next/confidence derivations
  → next-action recommendation            ← generic “Continue in mission workspace.”
  → follow-up / waiting / escalation      ← no policy
```

---

## Can the current system already reason over the Backus human open?

**Store and label: yes. Reason as an operator: no.**

If the Backus `opened` observation is on the mission (production-validated; #594/#595 made that durable):

- `Engine.inspect().observations` can include a memory line such as `Emmett   brevo opened for prospect <id>`.
- Timeline may include the observation event and, if side-effects hydrated, `Interpreted: human_open`.
- Asking “what changed?” can surface recent timeline labels.
- Asking “what happens next?” does **not** mention the open; it returns continue-in-workspace.
- Asking “why this confidence?” cites Scout discovery, not the human open.
- Workspace mode is Complete, not an observing operator surface.

Max cannot currently say, from mission state: “Backus opened as a human, that is not buying intent, wait N days then recommend an approved follow-up.”

---

## Current behavior (summary)

1. Canonical observations exist and are idempotent (`obs_<provider_event_id>`).
2. Human vs proxy is typed at interpretation; both are evidence-only; neither is buying intent.
3. Bounce / unsubscribe / Riley-classified replies can create outcomes and auto-advance to LEARN.
4. Opens do not create outcomes, do not change confidence, do not change candidate belief, do not change next action, and do not start a follow-up clock.
5. Max inspects missions only when asked; OBSERVE evidence is not an inspection input for next-action or confidence.
6. No automatic external action is taken on opens (autosend remains off).

---

## Exact files / functions

| Role | File | Functions |
|---|---|---|
| Webhook entry | `utils/brevoEvents.js` | `insertBrevoEvent`, `persistCanonicalProviderEvent` |
| Observation consumer | `services/acquisitionMissionProviderObservation.js` | `consumeMissionProviderEvent`, `tryMissionStageSideEffects`, `backfillMissionObservationsFromProviderEvents` |
| Durable observation write | `services/acquisitionMissionPersistence.js` | `persistProviderCommunicationObservation`, `persistObservation` |
| Observation contract | `packages/acquisition-mission/CommunicationObservation.js` | `createCommunicationObservation`, `isCommunicationEvidenceEventType` |
| Taxonomy / outcomes | `packages/acquisition-mission/ObservationInterpretation.js` | `interpretMissionObservation`, `interpretEngagementObservation`, `shouldCreateOutcome`, `hasMeaningfulBusinessOutcome` |
| Engine apply | `packages/acquisition-mission/Engine.js` | `recordCommunicationObservation`, `applyCommunicationObservationInterpretation`, `applyInterpretationResult`, `tryAutoAdvanceToLearn`, `inspect`, `answerOperator` |
| LEARN gate | `packages/acquisition-mission/LearnProgression.js` | `shouldProgressToLearn`, `tryProgressToLearn` |
| Lifecycle gates | `packages/acquisition-mission/Lifecycle.js` | `canEnter` OBSERVE/LEARN, `derivePendingOperatorDecisionForStage` |
| Max inspect (when asked) | `packages/acquisition-mission/Inspection.js` | `inspectQuestion`, `explainNext`, `explainConfidenceBasis`, `explainRecommendation` |
| Workspace inspect routing | `packages/max/workspace/WorkspaceMissionInspection.js` | `inspectActiveMission` |
| Conversation facts | `packages/max/workspace/ConversationLayer.js` | `extractMissionFacts` (Scout/stage/pending; no observation types) |
| Workspace mode | `packages/acquisition-mission/WorkspaceMode.js` | `TERMINAL_STAGES` includes `observe` → mode Complete |
| Riley replies | `services/acquisitionMissionRileyInterpretation.js` | `consumeRileyReplyInterpretation` |
| Hydrate (no interpretations table) | `services/acquisitionMissionRuntime.js` | `hydrate`, `persistMissionState` |

Not on this path: `utils/maxWarmthScoring.js`, `utils/maxOrchestration.js`, `maxAgent.js`.

---

## Tests covering OBSERVE

| File | What it covers | What it does not cover |
|---|---|---|
| `test/communicationObservation.test.js` | Observation contract; webhook → observation; EXECUTE → OBSERVE; idempotent ids | Max inspect / follow-up / confidence |
| `test/observationInterpretation.test.js` | `human_open` / delivered / clicked / replied create **no** outcome; bounce/unsubscribe do; Riley classifications; LEARN after unsubscribe | Next-action from open; human vs proxy affecting anything beyond type |
| `packages/acquisition-mission/tests/specObserveLearnProgression.test.js` | Transport-only stays OBSERVE; interested/negative/booking auto LEARN | Follow-up policy |
| `packages/acquisition-mission/tests/specLearnImproveProgression.test.js` | OBSERVE → LEARN regression | Operator reaction |
| `test/anchorBackusProviderObservation.test.js` | Backus sent/opened durable observations; prospect/execution binding; backfill; audit query | Max reasoning over the human open |
| `packages/acquisition-mission/tests/amo.test.js` | Generic `inspect().observations` memory lines; confidence still “No campaign results yet” without outcomes | OBSERVE event types |

No test asserts: Max inspects a new observation; an open changes confidence or candidate belief; a follow-up recommendation after human open; wait vs escalate policy; human open stronger than proxy for operator decision.

---

## Constraints preserved (observed, not proposed)

- Autosend is not triggered by this path.
- Opens are not buying intent.
- Human approval still gates execute; OBSERVE currently creates no new approval card.
- Canonical mission observations remain the evidence store.
- Scout / Paige / Emmett contracts are not involved after send.

No fix is proposed in this audit. The next design step, if taken, starts at: **Max operator inspection of new canonical observations while remaining in OBSERVE**, without automatic send.

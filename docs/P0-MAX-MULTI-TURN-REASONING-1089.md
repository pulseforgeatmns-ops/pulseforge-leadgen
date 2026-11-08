# P0 #1089 — Max multi-turn reasoning and routing

## Symptom
Tenant 10 (Anchor Cleaning), one workspace conversation, Nov 8 2026:

1. *"Give me a read-only operational status report for Anchor Cleaning …"* → correctly owned by `OperationalInspection` (#1088): 1 active, 47 stale, 8 uncertain, 1 completed mission; 211 prospects; 0 email touchpoints in 7 days; gate `maintenance (recovery_kill_switch)`; six sources read.
2. *"Using the operational evidence you just retrieved for Anchor Cleaning, identify the single canonical business objective … Explain why outbound is intentionally paused … Do not change session settings, approve or resume missions, alter the outbound gate, create new missions, or execute anything …"* → Max replied:

   > This approval is the next operator gate before the mission can advance.
   >
   > Authorize external execution of prepared outreach?

## Reproduction
`packages/max/workspace/tests/p0MaxMultiTurnReasoning1089.test.js` drives both turns verbatim through `WorkspaceEngine.ask()` against a mission sitting at `execution_approval` (bound, or only hydrated) plus a production-shaped persisted portfolio. On `main` the trace for turn 2 was:

| Step | Result on `main` |
|---|---|
| `maybeHandleOperationalInspectionTurn` | no match: turn 2 has no "status report" cue, so ownership ended after turn 1 (hypothesis **A**) |
| `resolveExecutionContract` → objective | `mission_execution`. Segment *"Do not … approve or resume missions … or execute anything"* matched `isApprovalMessage` / `isMissionExecutionCommand` on the **raw** text (hypothesis **C**); segment *"… recommend the **next** reliability validation step"* matched the bag-of-words `/\bnext\b/` execution command |
| AMO active mission | resolved by hydration even when the session was not bound to it (hypothesis **E**) |
| `pendingDecisionCaptureGuard` | `pending_decision_clarification`: `/\bwhy\b.*\bapprov/` spanned "Explain **why** outbound … do not **approv**e" |
| `resolvePendingOperatorDecision` | `QUESTION`, unresolved → `pendingDecisionOwnsTurn` = true (hypothesis **B**) |
| `PendingDecisionTurn.contextualPendingAnswer` | the same `why … approv` regex chose *"This approval is the next operator gate before the mission can advance."*, followed by the pending prompt |
| Transcript | the turn-1 report was only prose in the transcript; no structured evidence was available to any reasoning owner (hypothesis **D**) |

### Was anything mutated?
Not by the production wording. The mission store, pending decisions, execution-router audit and session execution policy were identical before and after turn 2 (asserted by the suite). **However**, the same investigation found a latent mutation: a constraint-qualified analytical turn such as *"Explain the outbound pause and separate the safety gate from execution failures. Do not approve or resume missions."* was resolved against a pending `execution_approval` as **REJECT → `cancel`** (executable). The suite reproduces that on `main` (`end to end WITHOUT prior evidence …`) and it is fixed below.

## Root cause
Two contracts were missing:

1. **No evidence-continuity / analytical-ownership contract.** Inspection ownership was purely lexical and per-turn. Nothing told routing that a follow-up was *about the evidence just retrieved*, so the turn fell to objective resolution and the pending-decision owner, which treat "a pending decision exists" as ownership.
2. **Negated clauses leaked into decision routing.** `NegatedDirective` (SPEC-MAX-RELIABILITY-004) was applied to mission *creation* detection, but not to mission-*execution* detection, the pending-decision capture guard, the decision-question classifier, or the contextual pending answer.

## Fix

### 1. Operational evidence continuity (`OperationalEvidenceContinuity.js`)
- **Evidence record.** Every successful inspection stores `session.operationalEvidence` (on the session object, like `sessionState`, so an envelope context switch does not drop it and it is not echoed in API responses): tenant id/name, as-of, mission classifications (id, title, objective, stage, status, category, basis, blockers), tenant blockers, gate mode/reason, aggregate prospect/touchpoint/agent counts, and per-source provenance/availability. No prospect rows.
- **Ownership contract** (`detectOperationalAnalysisFollowUp`). After negated clauses are stripped, the turn must carry **no** action command (`hasAffirmativeActionCommand`, shared with inspection) and **no** decision-response vocabulary (approve/authorize/reject/cancel/go ahead/proceed/yes/confirm), and must **not** single out one mission ("this mission", a mission id from the evidence). Then it is an analysis turn when either:
  - (a) it explicitly refers to previously retrieved operational evidence ("the evidence you just retrieved", "based on that report", "the previous inspection") and asks for analysis or names the portfolio; or
  - (b) the session already holds a current/stale evidence record for this tenant **and** the turn asks an analytical question (identify/explain/why/which/recommend/…) about the portfolio (missions, outbound, gate, blockers, failures, canonical objective).

  (b) is stateful — the same words without a recorded inspection keep normal routing — so this is not a global keyword bypass.
- **Placement.** In `_askProduction`, immediately after the inspection check: before objective resolution, session-state directives, the compound planner, mission-first routing, pending-decision handling, and AMO execution. A pending approval is context for the turn, not its owner.
- **Evidence resolution.** Same tenant and ≤ 30 min → reused and cited with its as-of time (only the client directory is re-read, for named-client authorization). Missing, older than 30 min, or recorded for another tenant → re-read through the same read-only inspection loader, and the answer says why. A named client that is not the session tenant is refused before any other read.
- **Analysis** (`composeOperationalAnalysis`, deterministic, presented verbatim):
  1. *Canonical objective* — the objective persisted on the only active, non-synthetic mission (with its source); with several active missions it lists them and says choosing is the operator's call; the approved Client Blueprint goal is shown as *declared intent, not execution evidence*.
  2. *Relevance* — active non-synthetic = plausibly current; stale/completed/dev-test = historical (pending decisions on stale missions are called historical state); uncertain = ambiguous, needs operator confirmation.
  3. *Safety vs failure* — the gate's persisted mode/reason (an armed kill switch is a deliberate hold, not an error) is separated from the **operator-declared** rationale, from actual failures (agent_log error runs, failed missions), from activity data (0 touchpoints is consistent with the gate, not proof of failure or health), and from operator gates/governance holds (pending decisions, synthetic-evidence refusals, restrictions).
  4. *Next validation step* — a controlled Max reliability check with the gate untouched; never an approval, resume, or launch.
  5. *Evidence and limits* — every source ✓/✗ with provenance, reused-vs-refreshed basis, an explicit "Cannot verify" list, and the operator's imperative constraints echoed back as honoured.
- **Authority boundary.** The analysis owner never calls a mission runtime, approval, the execution router, the session-state resolver, or outbound control. Session writes: transcript message + evidence record. All reads go through `readOnlyQueryable`.

### 2. Negation-aware decision routing
- `OperatorObjectiveResolutionEngine`: mission-execution / approval detection reads the affirmative (negation-stripped) segment; a prohibition-only segment never selects `mission_execution`.
- `NegatedDirective.isConstraintQualifiedRequest`: a message whose prohibitions qualify a separate informational request (and that uses no decision vocabulary affirmatively) is not a decision response. `PendingDecisionResolver` returns `UNRELATED` (guarded) for it — this closes the REJECT/cancel misread. "Don't send it.", "No, do not approve.", "Why should I approve this?", "Approved." keep their meaning.
- `isQuestionAboutDecision`, the capture guard's clarification patterns, and `contextualPendingAnswer` match the affirmative text.

## Tracing
- `askPathTrace` branches: `operational_analysis_detection` (matched, reason, cues, evidenceState) and `owner_pipeline:operational_analysis` (reason, mutation boundary, evidence continuity block).
- Structured log `[OPERATIONAL_ANALYSIS]`: tenant, route, owner, detection reason/cues, evidenceUse (`reused`/`refreshed`), evidenceState, mission counts, pending decisions `{present, requested: 0, consumed: 0}`, source availability, mutation boundary. No prospect data, mission titles, or objectives.
- Response: `workspaceOwnership.owner = 'operational_analysis'`, `resolution.action = 'operational_analyzed'`, `operationalEvidence` (= `structured.metadata.operationalEvidenceContinuity`), `presentation = 'operational_analysis'`.

## Known limitations
- Ownership cues are still lexical within the stateful contract; an analytical follow-up phrased with none of the analytical or portfolio cues falls back to normal routing (which, after the negation fixes, no longer misreads constraints as decisions).
- The analysis is deterministic and limited to what the inspection reads (see `P0-MAX-OPERATIONAL-INSPECTION.md` limitations). It does not judge topical similarity between stale missions and the canonical objective; stale missions are historical by the documented 14-day rule.
- The evidence record lives on the in-memory workspace session (like the transcript); it is not persisted across process restarts — a follow-up after a restart refreshes the evidence and says so.
- The bag-of-words `MISSION_EXECUTION_COMMAND_RES` (e.g. `/\bnext\b/`) is unchanged; the analysis owner precedes it. Tightening it is out of scope for this P0.

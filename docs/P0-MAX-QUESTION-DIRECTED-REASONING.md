# P0 — Max question-directed reasoning over operational evidence

Predecessor: #1089 / PR #1090 (`P0-MAX-MULTI-TURN-REASONING-1089.md`).

## Symptom
Tenant 10 (Anchor Cleaning), one Command Deck conversation, Nov 8 2026. After a read-only inspection and a portfolio analysis, the operator asked:

> Why is the CRM preparation mission classified as stale, and what evidence would justify treating it as current again? Do not approve, reject, cancel, resume, or modify that mission. Explain only, using the evidence from your previous answer.

Max returned the full five-section portfolio analysis again (canonical objective, 48 stale / 8 ambiguous missions, outbound gate, 31 pending decisions, evidence limits) and never addressed `mission_crm_ddf50ad75bfdde8f77d7ed9b8b3bd83a`.

## Root cause
Traced through `WorkspaceEngine.ask()` → `maybeHandleOperationalAnalysisTurn`:

| Question | Finding |
|---|---|
| 1. Does the analytical handler always render a fixed portfolio-wide response? | **Yes.** `composeOperationalAnalysis(record)` took only the evidence record and always emitted the same five sections. |
| 2. Is the operator's question discarded after ownership? | **Yes.** `question` was used for ownership detection and named-client authorization, then only to echo negated constraints. Nothing about *what* was asked reached answer construction. |
| 3. Does the evidence record hold enough mission-level detail? | **Mostly.** It had id, title, objective, stage, status, category, basis, `lastActivityAt`, blockers — but not the timestamps behind the classification (created / updated / latest event), so it could not say *which* persisted fact made the mission stale. |
| 4. Can the handler distinguish portfolio analysis from mission explanation? | **No.** There was no notion of subject. |
| 5. Were mission-specific questions deliberately excluded? | **Partly.** `SPECIFIC_MISSION_REFERENCE` ("this mission", a mission id) declined ownership to avoid hijacking mission continuation. The production wording ("the CRM preparation mission") slipped past that regex and matched the explicit prior-evidence reference ("using the evidence from your previous answer") — so the turn was owned correctly, then answered with the fixed report. |
| 6. Can existing infrastructure answer safely? | **Yes, deterministically.** Everything the question needs is in the evidence record or one tenant-scoped read. No model call is needed, and the existing read-only guard (`readOnlyQueryable`) covers the one new read. |

The routing contract was right; the answer contract did not exist.

## Fix — separate ownership from answer construction

### 1. Analytical frame (`OperationalQuestionFrame.js`)
Ownership still decides *that* a turn is read-only analysis. A frame then decides *what* the analysis is about:

```
{ question, affirmative,            // verbatim text; text with negated clauses removed
  subject: 'mission' | 'portfolio',
  subjectIds, reference,            // resolved evidence mission ids; 'id' | 'description' | 'focus'
  unresolved, candidates,           // named missions that did not match / matched ambiguously
  facets,                           // what is asked, in the order asked
  intent,                           // explanation | diagnosis | evaluation | comparison | portfolio_overview
  responseScope: 'focused' | 'portfolio',
  actionAuthorized: false }         // the analytical route never carries authority
```

- **Mission resolution** is grounded only in the evidence record: exact id; or a description ("the CRM preparation mission", "the October 16 governed outbound mission") where every descriptive word must match the title/objective/id (loose stem) and any date must match a persisted date or the title. A tie produces `candidates` (Max asks which) — it never guesses.
- **Focus.** A mission answer stores `session.operationalFocus` (tenant-bound, ≤ 30 min). "it", "this mission", "that one", "both" resolve to it on the next analytical turn. It is cleared by a new inspection, by a portfolio answer, and by **any turn another owner takes**, so it cannot leak into mission continuation later.
- **Facets** (mission): classification, reconsideration, relevance, abandonment, history, comparison, and a default evidence brief. (Portfolio): objective, relevance, safety, recommendation, overview.

### 2. Ownership for mission subjects (`detectOperationalAnalysisFollowUp`)
A turn whose frame subject is a mission is analysis only when **all** hold:
- the session holds evidence for this tenant (or the turn explicitly references prior evidence);
- after negated clauses are removed, there is no action command, no decision-response vocabulary (approve/authorize/reject/cancel/yes/…), and no mission action verb (run/resume/cancel/archive/advance/continue/…);
- the turn is a question or an explicit request to explain/compare/describe.

A generic "this mission" with **no** analytical focus does not resolve to a subject, so it still declines (`specific_mission_reference`) and keeps mission ownership. "Let's run it", "Approve", "Resume the CRM preparation mission" all keep their existing authorization paths.

### 3. Mission reasoning (`OperationalMissionReasoning.js`)
Each facet is a small reasoning function over the mission's evidence that keeps four layers explicit, so a database classification is never presented as a business conclusion:

| Layer | Example (CRM mission) |
|---|---|
| Observed fact | last recorded activity 2026-10-25 22:41:53Z (latest mission event "Preparation committed"); stage ready / status Ready |
| Classification rule | non-terminal + no activity for > 14 days → stale; at inspection that was 14.0 days — just past the threshold, so the classification is marginal |
| Interpretation | parked, abandoned, superseded, or still relevant — persisted state does not distinguish them |
| Decision requirement | relationship to the current objective, lifecycle state, validity of prepared work, deliberate operator retention; an open approval or new activity alone is not enough; any change needs explicit operator authorization |

The answer opens with a direct conclusion, adds only the sections for the facets asked, and ends with a short evidence basis (reused vs refreshed vs read-now, unavailable mission sources, constraints honoured, "No changes were made."). It does not reproduce inventories, prospect totals, agent activity, or outbound statistics.

**Missing evidence.** A mission id not in the evidence is read through `OperationalInspection.loadMissionById` — `SELECT` only, through `readOnlyQueryable`, scoped `tenant_id = session tenant` — classified with the same criteria and disclosed as read now. Not found → "I cannot find a mission with id … in <tenant>'s persisted mission stores … I cannot verify anything about it." Another tenant's mission is indistinguishable from a missing one. An unmatched description asks for the id.

### 4. Proportional portfolio answers
`composeOperationalAnalysis` now takes the frame. A portfolio-wide request (no facet, an overview cue such as "full operational analysis", or ≥ 3 facets — e.g. the original #1089 summary request) gets the five numbered sections exactly as before. A narrower portfolio question ("Why is outbound paused?") gets the header, only the sections it asks about, and the evidence section.

### 5. Evidence record
Each mission entry now also carries `createdAt`, `updatedAt`, `completedAt`, and `lastEvent {kind, label, at}` so follow-ups can cite the fact behind a classification without re-reading.

## Authority boundary (unchanged)
The analysis owner never calls a mission runtime, approval, the execution router, the session-state resolver, or outbound control. Session writes: transcript, evidence record, and the analytical focus. All reads go through `readOnlyQueryable`. `pendingDecisions` remains `{present, requested: 0, consumed: 0}`.

## Tracing
- `askPathTrace` branch `operational_analysis_frame`: subject, intent, scope, facets, reference kind, subject count, unresolved kinds, ambiguity count, sections, lookup outcomes.
- `[OPERATIONAL_ANALYSIS]` log adds `frame` (same log-safe view — no titles, objectives, or question text).
- Response: `operationalEvidence.analyticalFrame`, `operationalEvidence.conclusions`, `operationalEvidence.uncertainties`.

## Tests
`packages/max/workspace/tests/p0MaxQuestionDirectedReasoning.test.js` — real `WorkspaceEngine.ask()` with a production-shaped persisted portfolio (no active missions, stale CRM + governed outbound missions with pending execution approvals, other-tenant mission): A exact production sequence, B relevance, C stale ≠ abandon, D comparison, ambiguity, E missing evidence (read-now / not found / unmatched description), F continuity across four turns and focus expiry, G approval isolation on the named mission, H explicit execution approval still dispatches, I full portfolio still available, J tenant isolation. The #1089 suite's narrow follow-ups now assert proportional sections.

## Known limitations
- Facet and mission-description detection is lexical. A question with no recognisable facet gets the default evidence brief for the mission (classification + recorded history), not a refusal; a description that matches nothing asks for the id.
- Reasoning is deterministic. It does not judge whether two objectives describe the same work; it shows both and reports shared terms as textual overlap only. An LLM synthesis layer could later be placed over the same frame and facts, but must stay read-only and receive only the frame's evidence.
- The inspection reads the latest event per mission, not the full event history; "what happened" answers say so.
- Mission action verbs in a question ("What would justify resuming it?") are treated conservatively and fall back to normal routing (which, after #1089, does not misread negated or interrogative text as a decision).
- Focus and evidence live on the in-memory workspace session; after a process restart, evidence is refreshed and pronoun references must be restated.

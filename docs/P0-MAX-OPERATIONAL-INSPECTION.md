# P0 #1087 — Max read-only operational inspection

## Symptom
Operator (tenant 10, Anchor Cleaning) asked Max:

> Give me a read only operational status report for anchor cleaning. Identify the current mission state, active execution blockers, and supporting evidence. Distinguish active business missions from completed, stale, or development/test mission, but do not archive, delete, or modify anything.

After #1086 Max answered "Mission Updated — Acquisition Mission Planning", Discovery, 40 %, confidence 0.84, `Sources: Blueprint`, "Approve discovery?".

## Root cause (exact path)
`POST /api/v1/max/workspace/ask` → `getMaxRuntime().askWorkspace()` → `WorkspaceEngine.ask()` → `_askProduction()`:

1. `resolveSessionState()` matched "read only" as a session directive and set the **session** `executionPolicy = read_only` (sticky; later approvals in the same session were then refused).
2. Objective resolution returned `general_conversation`; #1086's `IntentExtractor` change kept the turn out of the compound planner.
3. No active SPEC-022 mission → `maybeHandleMissionFirstTurn` returned null. With an active AMO mission, `PendingDecisionResolver` guarded the turn as an inspection question (no approval was consumed).
4. Ownership: the read-only conversation contract selected `conversation_identity`; identity did not handle it → `WORKSPACE_OWNER_FALLBACK` to `reasoning`.
5. Reasoning fallback → `maybeHandleClientIntelligenceTurn` → `composeClientContextReasoning` in `unknowns` mode (`claimedBy: client_intelligence_unknowns`) → `MissionCommunication.buildUnknownsMissionCommunication()`, which **hard-codes** `headline: 'Mission Updated'`, `stage: 'Discovery'`, progress ≤ 40 %, `confidence: 0.84`, `sources: ['Blueprint']`, `operatorDecision: 'Approve discovery?'` from approved-Blueprint fields.

No route existed that reads persisted operating state for a portfolio-level status request.

### Was "Mission Updated" a persisted mutation?
No. Reproduced in `p0OperationalInspection1087.test.js` and a scratch trace with and without an active AMO mission: the acquisition-mission store (missions, events, contributions) was byte-identical before and after. The card was presentation fabricated from Blueprint content. The real side effect was step 1: a sticky session `read_only` policy.

## Fix
`packages/max/workspace/OperationalInspection.js`, invoked in `_askProduction` immediately after review navigation — **before** objective resolution, session-state directives, the compound planner, mission-first routing, pending-decision handling, AMO execution, and reasoning fallback.

- **Detection** (`detectOperationalInspection`): "operational status/report/…", or "status report" plus a read-only or portfolio cue, or a read-only + portfolio + state cue. Negated clauses ("do not archive…") are stripped first; any remaining action command (approve, execute, launch, run Scout, create mission, …) leaves the turn to normal routing. "What's the status of this mission?", "Approved.", "Switch to read-only mode." are not matched.
- **Tenant**: `session.context.tenantId` only (verified against the authenticated session by `routes/maxWorkspace.js`, which also 403s a mismatched `context.tenantId`). If the question names a different known client (from `clients`), the turn is refused before any other tenant data is read.
- **Read-only boundary**: every SQL statement goes through `readOnlyQueryable`, which rejects anything but plain `SELECT`/`WITH` reads (and `FOR UPDATE/SHARE`). No mission runtime method, approval, router, or Blueprint is called. Session writes are limited to the transcript messages. The `mutationBoundary` (reads, writesBlocked) is returned and logged.
- **Sources** (each with provenance, as-of, availability): `acquisition_missions` + `acquisition_mission_events` (or the in-process runtime, labelled non-durable, when no DB is configured); `missions` (SPEC-022); `prospects` (aggregates only); `touchpoints` (by channel, 7 d / 30 d / last); `agent_log` (last run, 7 d runs/errors); Anchor outbound gate via `anchorEmailRecovery.loadRecoveryState` (tenant 10 only).
- **Presentation**: `PresentationEngine` returns the report verbatim (`metadata.operationalInspection`), so a model cannot invent progress or drop unavailable-source disclosures.
- **Trace**: `askPathTrace` branch `owner_pipeline:operational_inspection`; structured log `[OPERATIONAL_INSPECTION]` with tenant, route, detection reason, mission counts, source availability and mutation boundary — no prospect data, names or objectives.

## Classification criteria (in precedence order)
1. **Development / test** — only from persisted markers: `syntheticEvidence.provenance = 'synthetic_fixture'` or an explicit test/development flag. Never from name or age (`docs/roadmap/MAX-MISSION-LIFECYCLE-HYGIENE.md`).
2. **Completed / closed** — persisted status complete/completed/reviewed/done, or cancelled/archived/abandoned/superseded/retired, or `planCancelled`.
3. **Uncertain** — non-terminal and the title/objective looks like test residue without a persisted marker, or no activity timestamp is recorded.
4. **Stale** — non-terminal, last recorded activity (max of `updatedAt`, last mission event, `completedAt`) older than 14 days.
5. **Active** — non-terminal with recorded activity within 14 days.

Blockers per mission: persisted `blockers`, pending operator decision, synthetic-evidence outreach refusal, durable operator restrictions. Tenant blockers: outbound gate not active, agent error runs in the last 7 days.

## Known limitations
- The Anchor outbound gate is the only tenant-specific gate read; other governed state (research-only grants, scoped resumes) is not yet summarized.
- Named-client refusal depends on the `clients` directory being readable; when it is not, the session tenant is still enforced and the check is reported as `unavailable` in the snapshot.
- Sticky session `read_only` from an explicit "use read-only mode" directive is unchanged (intended); only inspection turns no longer set it.

## Follow-up turns (P0 #1089)
Inspection evidence is recorded on the session and analytical follow-ups over it ("Using the operational evidence you just retrieved …") are owned by `OperationalEvidenceContinuity.js`, ahead of pending decisions. See `docs/P0-MAX-MULTI-TURN-REASONING-1089.md`.

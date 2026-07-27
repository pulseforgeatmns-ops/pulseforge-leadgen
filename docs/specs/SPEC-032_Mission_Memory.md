# SPEC-032 — Mission Memory

| Field | Value |
|---|---|
| **Status** | Proposed |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v0.1.0 |
| **Depends on** | SPEC-022 (Mission Engine), SPEC-023 (Capability Framework), ADR-001, ADR-003, ADR-010, ADR-011, ADR-019 |
| **Note** | Product draft was labeled “SPEC-031”; renumbered to SPEC-032 because SPEC-031 is Business Signals Capability. Implementing ADR was labeled “ADR-018”; repository **ADR-019** is Missions Are Conversations (ADR-018 remains Time Matters). |

## Objective

Make every Mission a **persistent collaborative workspace** where the operator and Max iteratively refine an objective until it is ready for execution.

The operator should never need to restart a Mission simply to make corrections. Follow-up messages stay attached to the active Mission. Corrections update Mission state, append revisions, and optionally rerun capabilities against the current revision. Execution begins only after explicit operator approval of the latest revision.

Success looks like: “Use Manchester instead of Boston” → Mission updated → capabilities regenerated → workspace shows the decision and revision — no new Mission, no lost context.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Product_Experience.md`
- [ADR-019](../adr/ADR-019_Missions_Are_Conversations.md) — Missions are conversations; capabilities consume current revision
- [ADR-001](../adr/ADR-001_Conversation_First.md) — conversation as the control surface
- [ADR-003](../adr/ADR-003_Human_Approval.md) — no automatic execution / outreach
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine + Mission-First UX
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md) — Mission Planner / Executor / Workspace thin slice
- [SPEC-023](SPEC-023_Capability_Framework.md) — Capability Registry & runner
- [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md) — Max Ask / workspace presentation

## Problem

Today the interaction model still collapses toward:

```text
User → Request → Response → Finished
```

SPEC-022 made Missions durable and introduced Mission Workspace, but it does not yet guarantee:

- Follow-ups attach to the **active** Mission instead of spawning a new one
- Corrective language (“Actually…”, “Instead…”, “Use Manchester…”) modifies Mission context in place
- Append-only **revision history** when Discovery Profile, constraints, or plan change
- A single workspace that owns conversation, decisions, plan, evidence, and revisions
- Capability reruns that always read the **current** Mission revision
- Explicit Ready-for-Review → Approve / Reject / Revise / Archive before execution

Without Mission Memory, operators restart Missions to fix geography, volume, or exclusions — losing conversation, decisions, and evidence. That violates Conversation First and undermines Mission-First UX.

## Guiding Principle

**Target:**

```text
User → Mission → Conversation → Refinement → Approval → Execution
```

A Mission remains the active context until it is completed, cancelled, or explicitly abandoned.

## Scope

### Mission Context (owned by every Mission)

- Objective
- Current plan
- Discovery Profile
- Client Playbook
- Capability outputs
- Operator decisions
- Revision history
- Approval state

Follow-up messages automatically attach to the active Mission.

### Conversation model

Example (single Mission throughout):

```text
User: Build Campaign 001
  → Mission Created
Operator: Use Manchester instead of Boston.
  → Mission Updated
Operator: Increase target count to 75.
  → Mission Updated
Operator: Exclude restaurants.
  → Mission Updated
Operator: Looks good. Approve.
  → Execution
```

No new Mission is created for corrections.

### Routing

If an active Mission exists:

1. All conversational requests first attempt to **modify** the active Mission.
2. Only create a new Mission when:
   - User explicitly requests one
   - User changes objective entirely
   - Operator selects “New Mission”

### Mission Workspace (canonical conversation)

| Section | Contents |
|---|---|
| Overview | Objective, Status, Progress |
| Conversation | Every operator interaction and Max response, chronological |
| Decisions | Every accepted change (e.g. Discovery Profile Boston → Manchester) |
| Plan | Current execution plan (live) |
| Evidence | Evidence attached to the Mission (not scattered across capabilities) |
| Revisions | Every capability rerun; operator can compare and restore |

### Revision Engine

Changing Mission context **never mutates history**.

```text
Revision 1 — Boston
Revision 2 — Manchester
Revision 3 — 75 Prospects
```

Operator can restore any revision. Restoring creates a new head revision (append-only), not an in-place overwrite of the past.

### Smart Corrections

Max recognizes corrective language and routes it as Mission modification, not restart:

- Actually… · Instead… · Change… · Update… · Use… · Remove… · Add… · Increase… · Decrease…

### Clarification Engine

If a correction creates ambiguity, Max asks before applying:

```text
User: Use Manchester.
Max: Do you mean Manchester, NH or Manchester, UK?
```

### Capability integration

Every capability becomes **rerunnable** against updated Mission state:

```text
Discovery → Company Intelligence → Ranking → Campaign → Proposal
```

All reuse the current Mission revision (profile, playbook, constraints, prior outputs as inputs where valid).

### Approval

Execution never begins automatically. Mission enters **Ready for Review**. Operator chooses:

- Approve
- Reject
- Revise
- Archive

### Mission state

```text
Draft → Planning → Review → Approved → Executing → Completed → Archived
```

(Align with / extend SPEC-022 lifecycle; Review maps to Ready for Review; cancel/abandon paths remain.)

### Operator experience

Instead of a dead-end like “Market Intelligence unavailable,” Max responds with context-preserving updates, e.g.:

> Good catch. Anchor Cleaning serves the Greater Manchester, NH area. I've updated the Mission to use the Commercial Cleaning – Manchester Discovery Profile and regenerated the campaign.

Mission updates immediately. No loss of context.

### Learning (record now; recommend later)

Mission Memory records for future recommendations:

- Common corrections
- Frequently changed profiles
- Approval patterns
- Revision counts

v1 persists the signals; automated recommendations from them are Future Work.

## Out of Scope

- Autonomous execution / outreach without Approve (ADR-003)
- Cross-tenant Mission sharing
- Replacing SPEC-022 planner determinism with open-ended LLM-only planning
- Full semantic objective-change detection beyond v1 heuristics + clarification (deep NLU deferred)
- Auto-apply restore without operator confirmation
- Learning-driven auto-recommendations in v1 (record only)
- Rewriting every legacy agent button or chat path unrelated to Mission routing

## Dependencies

- SPEC-022 Mission Engine (planner, executor, durable missions, Mission Workspace shell)
- SPEC-023 Capability Framework (registry, runner; capabilities must accept Mission revision context)
- ADR-019 Missions Are Conversations
- ADR-001 Conversation First · ADR-003 Human Approval · ADR-010 Mission Engine · ADR-011 Capability Framework
- Max Ask / IntentRouter (SPEC-009 + SPEC-022) — active-Mission preference
- Command Deck Mission Workspace UI (SPEC-008 / SPEC-022 addendum)
- Downstream capabilities: Discovery (SPEC-024), Company Intelligence (SPEC-030), Ranking (SPEC-026), Campaign Builder, Proposal (SPEC-027B), Execution (SPEC-029)

## Architecture

```text
Operator message
       ↓
 Active Mission? ──no──→ MissionPlanner (new Mission)
       │ yes
       ↓
 Correction / refinement / clarify?
       │
       ├─ ambiguous → Clarification Engine → wait
       ├─ new objective / explicit New Mission → MissionPlanner
       └─ modify → Revision Engine (append revision)
                        ↓
                 Update MissionMemory
                        ↓
                 Rerun affected capabilities (current revision)
                        ↓
                 Mission Workspace (canonical UI)
                        ↓
                 Ready for Review → Approve → Execution (approved revision only)
```

### Mission Memory model

```ts
interface MissionMemory {
  missionId: string;
  objective: string;
  conversation: MissionMessage[];
  currentRevision: number;
  revisions: MissionRevision[];
  decisions: MissionDecision[];
  operatorCorrections: OperatorCorrection[];
  approvals: MissionApproval[];
  activeArtifacts: MissionArtifactRef[];
}
```

Capabilities and Execution **must** read `currentRevision` (or the explicitly approved revision id for execution), never the original request text alone.

## Data Model

Extend SPEC-022 durable mission storage (additive; no silent history rewrite):

| Concept | Persistence notes |
|---|---|
| `mission_messages` | Chronological conversation (operator + Max); `mission_id`, `role`, `body`, `created_at`, optional `revision_id` |
| `mission_revisions` | Append-only snapshot of objective, profile/playbook pins, constraints, plan hash, capability output refs; `revision_number`, `created_by`, `reason` |
| `mission_decisions` | Accepted changes (field, from → to, revision_id, operator/max) |
| `mission_approvals` | Review actions: approve / reject / revise / archive; ties to `revision_id` |
| `missions` columns | `current_revision`, `active` / lifecycle status, optional `abandoned_at` |

Tenancy: all rows scoped by existing mission `client_id` / tenant rules from SPEC-022. Idempotency: message and revision ids stable; restores append a new revision referencing `restored_from_revision`.

## Implementation Plan

1. **ADR + indexes** — ADR-019 Accepted; this spec Proposed; README / DECISIONS / CURRENT_STATE linked.
2. **Schema** — migrations for messages, revisions, decisions, approvals; backfill revision 1 from existing missions.
3. **MissionMemory service** — load/update active mission; append revision; record decision; restore.
4. **Routing** — IntentRouter / Max Ask: prefer active-Mission modify; New Mission only on explicit / objective-change / UI action.
5. **Smart Corrections + Clarification** — corrective-language heuristics; ambiguity prompts before apply.
6. **Capability rerun** — MissionExecutor re-invokes affected steps with current revision context; preserve prior revision artifacts.
7. **Workspace UI** — Overview, Conversation, Decisions, Plan, Evidence, Revisions (compare / restore).
8. **Approval gate** — Ready for Review; Approve binds Execution to approved revision id.
9. **Learning hooks** — persist correction/profile/approval/revision metrics (no auto-recommend yet).
10. **Tests** — routing, revision append/restore, no spurious Mission create, capability rerun inputs, approval-before-execute.

## Migration Strategy

- Forward-only additive tables/columns on existing `missions`.
- Existing missions get `current_revision = 1` and a synthetic revision snapshot from current envelope/plan.
- Rollback: feature-flag Mission Memory routing off; fall back to SPEC-022 create-on-intent behavior; revision tables remain inert.
- Compatibility: Mission API continues to expose SPEC-022 fields; Memory fields additive on `GET /api/v1/missions/:id`.

## Testing

- Unit: corrective-language classification; clarification triggers; revision append/restore invariants
- Integration: follow-up attaches to active Mission; “New Mission” creates; objective-change creates; Approve required before Execution
- Capability: rerun Discovery/Ranking (or stubs) sees updated profile/constraints from current revision
- UI smoke: Workspace sections show conversation, decision trail, revision compare
- Regression: SPEC-022 lifecycle and review still pass with flag off

## Acceptance Criteria

- [ ] Follow-up messages stay attached to the active Mission
- [ ] Corrections modify the current Mission (no unnecessary Mission recreation)
- [ ] Full append-only revision history; operator can compare and restore
- [ ] Mission Workspace is the single source of truth (Overview, Conversation, Decisions, Plan, Evidence, Revisions)
- [ ] Every in-scope capability can rerun using updated Mission state (`currentRevision`)
- [ ] Operator always reviews before execution (Approve / Reject / Revise / Archive)
- [ ] Ambiguous corrections trigger Clarification Engine before apply
- [ ] Execution references the latest **approved** revision only
- [ ] Learning signals (corrections, profile changes, approvals, revision counts) are recorded

## Future Work

- Learning-driven recommendations from Mission Memory patterns (SPEC-021 alignment)
- Richer objective-change / multi-Mission disambiguation via NLU
- Side-by-side revision diff UX beyond v1 compare
- Cross-Mission “apply this correction pattern” suggestions
- Streaming conversation tokens in Workspace (presentation only)

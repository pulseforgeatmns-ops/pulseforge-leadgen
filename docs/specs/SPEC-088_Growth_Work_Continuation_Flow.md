# SPEC-088 — Growth Work Continuation Flow

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High (P1) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-09 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-086](SPEC-086_Growth_Conversation.md); [SPEC-087](SPEC-087_Growth_Infrastructure_Readiness.md) |

## Objective

Make **Resume Growth Plan** continue a guided workflow: one click opens the Growth Workspace on the first incomplete task, then advances automatically. Readiness Report becomes an informational workspace tab — never an intermediate dead end.

> Product brief title used “SPEC-085”; repository SPEC-085 is already Executive Business Brief, so this work is numbered **SPEC-088**.

## Problem

`Continue Growth Work` opened the Growth Infrastructure Readiness side panel whenever a readiness report existed. Operators had to infer the next action. Growth Work did not behave like other guided Pulseforge workflows.

## Scope

- Rename CTA to **Resume Growth Plan** (or Continue Growth Plan)
- Growth Plan model derived from approved CIE session state
- Resume algorithm: load plan → first incomplete task → navigate there
- Growth Workspace with tabs: Overview · Tasks · Readiness Report · Blueprint · History
- Mark setup tasks complete → auto-advance
- Completion options when all recommendations are done
- Dashboard: current plan + progress; previous plans collapsed

## Out of Scope

- Autonomous campaign launch
- Mutating DNS/GBP/social without approval
- Rewriting approved Blueprints
- Multi-operator task assignment

## Architecture

```text
Dashboard / Client Intel
  → Resume Growth Plan
  → GET …/sessions/:id/resume
  → growthPlan + resumeTarget=growth_workspace|growth_complete
  → Growth Workspace
       → auto-open currentTask
       → Complete → POST …/growth-plan/tasks/:taskId/complete
       → next incomplete task (or completion options)
```

Readiness Report remains available under the **Readiness Report** tab.

## Data Model

Persisted on `interview_state.growthWork` (JSONB):

| Field | Meaning |
|---|---|
| `completedTaskIds` | Operator-completed task ids |
| `history` | `{ taskId, title, completedAt, note, source }[]` |
| `activeTaskId` | Last focused task (optional) |
| `updatedAt` | ISO timestamp |

Tasks are derived (not a new table): growth conversation milestone, infrastructure readiness milestone, readiness `recommendedSetupSequence` gaps, then optional “choose next objective”.

## Acceptance Criteria

- [x] Resume Growth Plan navigates to the first incomplete task (Growth Workspace)
- [x] Readiness Report is a workspace tab, not the continue destination
- [x] Continue never lands on a report-only dead end
- [x] Fully completed plans show completion options
- [x] Historical approved plans collapse under Previous Plans; current plan is primary
- [x] Button copy communicates resume of a guided plan

## Testing

- `test/clientIntelligenceGrowthPlan.test.js`
- Updates in `test/clientIntelligenceSavedSessions.test.js`

## Future Work

- Deep-link task ids in the URL
- Live connector checks that auto-complete Max-checkable setup items
- Campaign Builder handoff from completion options

# SPEC-045 — Command Deck UX Polish

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-008, SPEC-009, SPEC-022, SPEC-042, SPEC-043, ADR-030 |
| **Consumed by** | Max Command Deck, Mission Workspace, Operator Composer, Review Experience |

## Objective

Elevate the Command Deck from a developer console into an operator workspace: linear, calm, professional, and operator-focused. Operators issue objectives and review work without thinking about artifacts, parsers, routing, or capability chains.

## Vision References

- `docs/vision/Mission.md`
- [ADR-030](../adr/ADR-030_Command_Deck_Is_an_Operator_Workspace.md) — Command Deck is an operator workspace
- [SPEC-008](SPEC-008_Command_Deck_UI.md), [SPEC-009](SPEC-009_Max_Intelligence_Workspace.md)
- [SPEC-043](SPEC-043_Operator_Artifact_Injection.md)

## Problem

Mission OS can execute complete business workflows; remaining friction is UX. Long chats push the composer off-screen, raw CSV dominates conversation, Mission Workspace exposes transport formats and pipeline metadata, and review requires scrolling artifact dumps.

## Scope

### In scope (v1)

Applies to Max Command Deck, Mission Workspace, Operator Composer, and Review Experience:

1. Persistent composer (full-height flex; conversation scrolls)
2. Sticky suggested actions above composer
3. Auto-growing prompt (≤ ~10 lines, then internal scroll)
4. Prospect List detection → attachment cards (raw preserved)
5. Reusable attachment card shell (expand / collapse / view)
6. Expandable objective in Mission Workspace
7. Business input summaries (counts / industry buckets when present)
8. Artifact summary cards; Developer Details for metadata
9. Stage metrics from existing plan/progress/payloads
10. Review dashboard summary + existing review actions
11. Chat `white-space: pre-wrap` / preserved formatting
12. Scroll management (send scrolls; avoid jump on suggestion redraw)
13. Stage loading bars from existing step statuses
14. Responsive layout (no clipped composer)
15. Keyboard: Enter send, Shift+Enter newline, Esc close

### Out of scope

- Mission execution, Artifact Bus, injection, or capability logic changes
- Industry classification beyond fields already on prospect rows
- Mutating Mission records via attachment remove
- `↑` previous-prompt recall (future)

## Architecture

Presentation-only. Summaries are derived in Command Deck UI from existing API payloads (`mission.objectiveText`, `operatorProspectList`, artifact bus snapshots, `plan.steps`, `stageReview`, review actions). No new tables or server contracts required for v1.

```text
Operator prompt
    ↓
Max Workspace (sticky composer + attachment cards)
    ↓
Mission Workspace (business summaries + review dashboard)
    ↓
Developer Details (optional metadata)
```

## Implementation Plan

1. Docs: SPEC-045 + ADR-030
2. Max panel: composer dock, auto-grow, sticky suggestions, scroll/keyboard
3. Chat attachment cards for ProspectList pastes
4. Mission Workspace operator view (objective, inputs, stages, artifacts, review, loading)
5. Responsive polish + acceptance smoke

## Testing

- Manual: long thread keeps composer/suggestions visible; paste list → card; Mission Workspace collapsed objective + summaries; Expand / Developer Details; Esc / Enter / Shift+Enter
- No MissionEngine test changes required (UI-only)

## Acceptance Criteria

- [x] Composer always visible; conversation scrolls independently
- [x] Suggested actions remain above composer
- [x] Prompt expands naturally to ~10 lines then scrolls
- [x] Prospect lists become attachment cards after detection
- [x] Objectives no longer dominate the workspace by default
- [x] Business summaries replace raw transport formats in the primary view
- [x] Stage metrics report business-oriented progress
- [x] Review dashboard summarizes campaign readiness
- [x] Scroll position preserved on suggestion redraw where practical
- [x] Desktop and laptop layouts avoid clipping the composer

## Future Work

- Additional attachment types (Campaign Brief, CRM Export, PDF)
- Previous-prompt recall (`↑`)
- Server-side presentation DTOs if client derivation becomes thin
- Evidence-first Review interaction — delivered in [SPEC-047](SPEC-047_Review_Workspace_Interaction_Layer.md) / [ADR-031](../adr/ADR-031_Review_Must_Be_Evidence_First.md)

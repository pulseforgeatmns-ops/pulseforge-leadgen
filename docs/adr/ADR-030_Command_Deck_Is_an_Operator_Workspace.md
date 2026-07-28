# ADR-030 — Command Deck Is an Operator Workspace

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-045](../specs/SPEC-045_Command_Deck_UX_Polish.md) |
| **Supersedes** | — |
| **Related** | [ADR-001](ADR-001_Conversation_First.md), [ADR-019](ADR-019_Missions_Are_Conversations.md), [ADR-029](ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md), [SPEC-008](../specs/SPEC-008_Command_Deck_UI.md), [SPEC-009](../specs/SPEC-009_Max_Intelligence_Workspace.md), [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) |

## Context

Mission OS can execute end-to-end business workflows. The Command Deck and Mission Workspace still surface transport formats (raw CSV), artifact producers, dependency graphs, and pipeline metadata in the primary operator path. That presentation forces operators to think like developers to complete ordinary work.

## Decision

1. **The Command Deck shall present business objectives, business inputs, business progress, and business outcomes.**
2. **Implementation details** — raw transport formats, artifact producers, dependency graphs, and pipeline metadata — **are secondary** and remain available only when explicitly requested (e.g. Developer Details).
3. **UX changes under this ADR must not alter Mission execution behavior.** Presentation derives from existing Mission / Artifact payloads.
4. Implementing contract: [SPEC-045 Command Deck UX Polish](../specs/SPEC-045_Command_Deck_UX_Polish.md).

## Rationale

Mission OS is an operating system for business execution, not a developer console. As capabilities mature, the interface should abstract implementation details and emphasize operator intent, actionable progress, and reviewable outcomes. That reduces cognitive load and lets non-technical operators run sophisticated workflows.

## Consequences

### Positive

- Operators issue objectives and review work without learning Artifact Bus vocabulary
- Raw CSV / JSON remain inspectable without dominating conversation or Workspace
- Presentation can evolve independently of Mission execution contracts

### Negative / tradeoffs

- Client-side derivation of business summaries may lag richer server DTOs later
- Hiding metadata increases dependence on clear primary-path copy when something fails

### Follow-ups

- [x] SPEC-045 v1 thin slice (composer shell, attachment cards, Mission Workspace summaries)
- [x] SPEC-047 / [ADR-031](ADR-031_Review_Must_Be_Evidence_First.md) — evidence-first Review interaction layer
- [ ] Optional server presentation DTOs if client derivation becomes brittle
- [ ] Additional attachment types beyond Prospect List

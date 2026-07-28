# ADR-019 — Missions Are Conversations

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-032](../specs/SPEC-032_Mission_Memory.md) |
| **Supersedes** | — |
| **Related** | [ADR-001](ADR-001_Conversation_First.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md), [SPEC-023](../specs/SPEC-023_Capability_Framework.md) |
| **Note** | Product draft labeled this “ADR-018”; repository **ADR-018** remains Time Matters ([SPEC-031](../specs/SPEC-031_Business_Signals_Capability.md)). |

## Context

[ADR-001](ADR-001_Conversation_First.md) established conversation as the primary interaction model. [ADR-010](ADR-010_Mission_Engine.md) made Missions the control plane for business objectives. The thin slice of [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) creates durable missions and a Mission Workspace, but operator follow-ups still risk restarting work: a correction like “Use Manchester instead of Boston” can spawn a new Mission or orphan context across chat and workspace.

That reintroduces the request/response trap:

> User → Request → Response → Finished

Operators should never restart a Mission simply to refine geography, volume, exclusions, or profile choice. Capabilities ([ADR-011](ADR-011_Capability_Framework.md)) must consume the **current** Mission revision, not the original utterance. Execution must still wait for explicit approval ([ADR-003](ADR-003_Human_Approval.md)).

## Decision

1. **A Mission is a persistent collaborative workspace**, not a one-time request. Max maintains conversational context until the Mission is completed, cancelled, or explicitly abandoned.
2. **Follow-ups attach to the active Mission** by default. New Missions are created only on explicit request, full objective change, or operator “New Mission.”
3. **Capabilities consume the current Mission revision**, never a frozen original request. Corrective language updates Mission state and may rerun affected capabilities.
4. **Changing context never mutates history.** Revisions are append-only; operators can compare and restore prior revisions.
5. **Execution always references the latest approved revision.** Planning and review may iterate freely; outreach and other customer-visible execution begin only after Approve on that revision.
6. **Mission Workspace is the canonical conversation** for that Mission — Overview, Conversation, Decisions, Plan, Evidence, Revisions — not a side transcript.

## Consequences

### Positive

- Corrections refine in place; operators keep context
- Aligns Conversation First (ADR-001) with Mission-First UX (ADR-010)
- Capability reruns stay coherent under one Mission id
- Revision history supports audit, restore, and learning
- Reaffirms human gate before execution (ADR-003)

### Negative / tradeoffs

- Active-Mission routing must distinguish corrections from new objectives (ambiguity → clarification, not silent wrong attach)
- Revision storage and UI add surface area beyond the SPEC-022 thin slice
- Learning from corrections is deferred until enough Mission Memory volume exists

### Follow-ups

- [ ] Implement [SPEC-032](../specs/SPEC-032_Mission_Memory.md) (memory model, revision engine, workspace sections, smart corrections, clarification)
- [x] Implement [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md) / [ADR-025](ADR-025_Active_Missions_Take_Precedence.md) — Active Mission Resolver before IntentRouter on Max Ask
- [ ] Wire capability reruns against `currentRevision` Mission state (partial via SPEC-039 modify stale+rerun)
- [ ] Record correction / approval patterns for future recommendations (post-v1 learning)
- Update CURRENT_STATE when Mission Memory ships

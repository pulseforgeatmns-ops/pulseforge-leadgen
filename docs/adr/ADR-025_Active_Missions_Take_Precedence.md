# ADR-025 — Active Missions Take Precedence

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md) |
| **Supersedes** | — |
| **Related** | [ADR-001](ADR-001_Conversation_First.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-019](ADR-019_Missions_Are_Conversations.md), [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md), [SPEC-032](../specs/SPEC-032_Mission_Memory.md), [SPEC-037](../specs/SPEC-037_Operator_Inbox.md) |

## Context

[ADR-019](ADR-019_Missions_Are_Conversations.md) established that Missions are persistent collaborative workspaces and that follow-ups should attach to the active Mission. [SPEC-032](../specs/SPEC-032_Mission_Memory.md) designs Mission Memory, revision history, and active-Mission preference.

The SPEC-022 thin slice still routes every Max Ask through [IntentRouter](../../packages/mission-engine/IntentRouter.js) first, then always calls `createFromObjective`. There is no Active Mission Resolver. Consequences observed in production-shaped flows:

1. Follow-ups such as “Investigate why Campaign Review failed…” match stage-name keywords and spawn a **new** `campaign_review` Mission instead of inspecting the existing Mission’s audit history.
2. IntentRouter remains the first routing layer, so conversational resume/modify/diagnose never runs.
3. Campaign Builder, Mail Package Generator, Campaign Review, and Direct Mail Execution remain sibling IntentRouter mission types — never continued as stages of one active Mission via conversation.

Without an architectural rule that **active Missions outrank intent classification**, Mission Memory cannot land: every matched keyword remains a new objective.

## Decision

1. **An active Mission always takes precedence over intent classification.**
2. **IntentRouter is responsible only for creating new Missions.** It must not classify follow-ups, diagnostics, or modifications against an active Mission.
3. **Once a Mission exists and is bound to the operator session**, all conversational interaction flows through the **Active Mission Resolver** until the Mission reaches a terminal state (completed, failed, archived, cancelled).
4. **New Missions are created only when** the operator explicitly requests a new Mission, changes objective entirely (classified as New Mission), or no active Mission is bound.
5. Implementing contract: [SPEC-039 Active Mission Resolver](../specs/SPEC-039_Active_Mission_Resolver.md). Memory / revision semantics remain [SPEC-032](../specs/SPEC-032_Mission_Memory.md).

## Consequences

### Positive

- Follow-ups stop spawning duplicate Missions
- Diagnostics attach to the Mission that failed instead of inventing a sibling
- IntentRouter scope shrinks to cold-start / explicit-new classification
- Aligns Conversation First (ADR-001), Mission Engine (ADR-010), and Missions Are Conversations (ADR-019)

### Negative / tradeoffs

- Resolver must distinguish Resume / Modify / Diagnose / New Mission (ambiguity → clarification, not silent wrong attach)
- Session binding (session → active Mission) must be durable and cleared on terminal / explicit clear
- IntentRouter keyword sets for stage names (“Campaign Review”) must not fire while an active Mission is bound

### Follow-ups

- [x] Implement [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md) (resolver before IntentRouter on all ask paths)
- [x] Wire session ↔ active Mission binding; clear on terminal / `clearActiveMission`
- [x] Route diagnostics and modifications without IntentRouter; attach via Mission audit (SPEC-032 Memory tables still Proposed)
- [ ] Durable Postgres binding store + soft-bind on session refresh
- [ ] Update ADR-019 follow-up when SPEC-032 Mission Memory ships
- Update CURRENT_STATE when Active Mission Resolver ships

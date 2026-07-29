# SPEC-057 — Execution Domain Routing

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Depends on** | SPEC-009, SPEC-022, SPEC-039, SPEC-055, SPEC-056; ADR-025, ADR-027, ADR-039 |
| **ADR** | [ADR-041 Operator Intent Selects Execution Domain](../adr/ADR-041_Operator_Intent_Selects_Execution_Domain.md) |

## Objective

Operator intent selects which Max subsystem handles a request. The active conversation supplies context only — it never selects the execution domain.

## Required Sequence

```text
Operator Input
  → Intent Understanding
  → Select Execution Domain
  → Select/Attach Context
  → Execute
```

**Never:**

```text
Active Conversation
  → Select Execution Domain
```

## Execution Domains

| Domain | Owns |
|---|---|
| `mission_execution` | Mission Engine create / plan / execute |
| `mission_diagnostics` | Mission Engine diagnostics path |
| `morning_briefing` | Morning Brief Q&A |
| `market_intelligence` | Market / competitor / monitor Q&A |
| `workspace` | Command Deck / workspace navigation |
| `general_conversation` | Help and residual conversation |

The selected domain owns the request. Previous conversations are preserved but cannot intercept.

## Context Selection

After domain selection:

1. **Reuse** compatible context when the domain matches the prior domain.
2. **Attach** mission focus when entering Mission domains.
3. **Switch** explicitly when the domain changes (deterministic switch message).
4. **Scope the answer corpus** so briefing copy cannot answer Mission or Market requests.

## Mission Requests

Any request classified as `mission_execution` or `mission_diagnostics` must:

1. Create or attach a **MissionIntent** (SPEC-055).
2. Invoke the **Mission Engine**.
3. Continue Intent Understanding → Evidence Planning (SPEC-056) → Capability Planning → Execution.
4. Surface **Mission Workspace** — never answer inside an existing briefing conversation.

## Acceptance Example

Operator (while Morning Brief is displayed):

> Run an end-to-end execution audit for Campaign 001.

Max must:

- Classify as **Mission Diagnostics**
- Invoke the Mission Engine
- Create/attach mission context
- Display Mission Workspace

Max must not:

- Answer from Morning Briefing
- Answer from Market Intelligence
- Continue the briefing conversation because it is active

## Implementation

| Module | Role |
|---|---|
| `packages/max/workspace/ExecutionDomain.js` | Intent Understanding → domain; context attach |
| `packages/max/workspace/WorkspaceEngine.js` | Ask pipeline follows domain sequence |
| `packages/mission-engine/IntentRouter.js` | Intent Understanding before keyword fallback |
| `packages/max/workspace/ResponseComposer.js` | Answer corpus scoped by domain |

## Out of Scope

- Changing Mission Planning / Evidence Planning internals
- Retiring Active Mission Resolver (still runs inside Mission domains)
- LLM-based domain classification

## Tests

`packages/max/workspace/tests/executionDomainRouting.test.js`
`packages/max/workspace/tests/missionRouting.test.js`

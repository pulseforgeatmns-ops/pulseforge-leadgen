# ADR-041 — Operator Intent Selects Execution Domain

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-057](../specs/SPEC-057_Execution_Domain_Routing.md) |
| **Related** | [ADR-025](ADR-025_Active_Missions_Take_Precedence.md), [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md) |

## Context

Operators open Max from Morning Brief, Market Intelligence, or Workspace. When they issued a mission objective while a Morning Brief conversation was active, Max answered from briefing context instead of invoking the Mission Engine.

The failure mode: sticky conversation context became the implicit execution owner. Intent Understanding and MissionIntent creation ran only after a keyword Mission gate — and when that gate missed, ResponseComposer answered from the briefing envelope.

## Decision

1. **Operator intent selects the execution domain.** The active conversation never does.
2. Routing follows: Operator Input → Intent Understanding → Select Execution Domain → Select/Attach Context → Execute.
3. Registered domains include Mission Execution, Mission Diagnostics, Morning Briefing, Market Intelligence, Workspace, and General Conversation.
4. Mission domains always create/attach MissionIntent and invoke the Mission Engine.
5. Context is attached *after* domain selection; previous conversations are preserved but cannot intercept.
6. Implementing contract: [SPEC-057 Execution Domain Routing](../specs/SPEC-057_Execution_Domain_Routing.md).

## Consequences

### Positive

- Mission requests work regardless of which conversation is displayed
- Domain switches are explicit and deterministic
- Mission Planning (SPEC-055/056) unchanged once the Mission domain is selected
- Briefing/Market surfaces remain available for their own domains

### Negative / tradeoffs

- Domain taxonomy must stay aligned with Intent Understanding categories
- Keyword mission seeds remain for types Understanding does not yet cover

### Follow-ups

- [x] ExecutionDomain selector + WorkspaceEngine wiring
- [x] Cross-domain routing tests
- [ ] Optional UI domain indicator on Command Deck

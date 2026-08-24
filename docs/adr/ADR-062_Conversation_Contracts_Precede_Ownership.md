# ADR-062 — Conversation Contracts Precede Ownership

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Spec** | [SPEC-155](../specs/SPEC-155_Conversation_Contract_Engine.md) |

## Context

Operator messages often establish conversational rules before stating a task: *don't execute*, *answer naturally*, *maintain context*. The workspace runtime previously resolved ownership and mission execution before honoring these constraints.

## Decision

The first responsibility of `WorkspaceEngine.ask` is determining the **Conversation Contract** via `resolveConversationContract()`.

Ownership is prohibited from selecting a mission owner until a Conversation Contract exists and `executionAllowed` is evaluated.

## Consequences

- Operator conversation rules are parsed once into durable session state
- `OperatorIntent` focuses on *what* the operator wants, not *how* the conversation is governed
- Mission runtime, presentation, and ownership all consume the same contract object
- Explicit contract updates (`"Let's execute"`, `"Switch topics"`) replace implicit re-parsing

## Alternatives Considered

**Embed rules in OperatorIntent** — rejected; mixes task intent with conversational governance (SPEC-153 separation violated).

**Re-parse in each subsystem** — rejected; duplicates detection and allows drift between ownership and presentation.

# ADR-068 — Session State Is Explicit

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Related** | [SPEC-148](../specs/SPEC-148_Session_State_Manager.md) |

## Context

Operators issue durable directives such as *"For the rest of this conversation, don't execute anything."* These are session-level constraints, not per-turn questions. Inferring session behavior from recent prompts caused Max to forget constraints after a single turn.

## Decision

1. **Session State is a first-class runtime object** stored on the workspace session.
2. **Session State precedes Conversation Contract** in the `WorkspaceEngine.ask` pipeline.
3. **Session behavior is never inferred solely from recent prompts** — inspection and enforcement read stored Session State.
4. **Every downstream subsystem receives Session State** — ownership, operator intent, mission runtime, and presentation must consult it.
5. **Session mutations emit history** — each field change records `SessionStateChange` with previous, current, reason, and timestamp.

## Precedence

```text
Explicit Session State
  ↓
Conversation Contract
  ↓
Current Prompt
  ↓
Historical Conversation
  ↓
Defaults
```

## Consequences

- Operator directives like execution bans persist until explicitly changed or reset
- Session inspection answers reflect stored state, not reconstructed intent
- Conversation Contract inherits execution constraints from Session State
- Mission runtime dispatch fail-closes when `executionPolicy` is read-only

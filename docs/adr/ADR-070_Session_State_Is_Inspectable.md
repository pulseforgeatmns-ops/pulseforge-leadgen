# ADR-070 — Session State Is Inspectable

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Related** | [SPEC-150](../specs/SPEC-150_Session_State_Inspection.md), [SPEC-148](../specs/SPEC-148_Session_State_Manager.md), [ADR-068](ADR-068_Session_State_Is_Explicit.md) |

## Context

SPEC-148 made Session State persistent and explicit. Operators can set operating mode, execution policy, reasoning mode, and conversation style for the remainder of a workspace session.

Inspection questions such as *"What operating mode are you currently using?"* still reconstructed those values through identity prose, business advisory, or mission context. Explicit runtime state was written once, then ignored on read.

## Decision

1. **Whenever the operator asks about the current operating session, Max answers by reading Session State.** Inspection never reconstructs operating mode, execution policy, reasoning mode, conversation style, or evaluation mode from the current prompt.
2. **Explicit runtime state takes precedence over inference.** Mission context, identity labels, and business advisory defaults cannot override stored Session State.
3. **`SESSION_INSPECTION` is a first-class message type.** It bypasses mission runtime, Scout, business advisory, and mission ownership.
4. **Inspection and explanation remain separate.** "What are you using?" reads Session State. "Why are you using it?" reasons over Session State as evidence and still does not infer the stored values.
5. **Any runtime component that mutates persistent Session State must expose that state through the same inspection interface.** Mutators write via `setSessionState`; inspection reads via `SessionStateManager.getCurrentState(session)`.

## Precedence

```text
Explicit Session State (read)
  ↓
Inspection response
```

Inference, identity labels, and business advisory are not consulted.

## Consequences

- Session configuration and session inspection are symmetrical: write once, read directly
- Inspection questions cannot create missions, invoke Scout, or produce acquisition recommendations
- Follow-up "why" questions execute reasoning but treat Session State as the source of truth
- Downstream mutators cannot hide session fields behind a private store

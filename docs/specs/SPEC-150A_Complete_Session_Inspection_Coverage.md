# SPEC-150A — Complete Session Inspection Coverage

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-24 |
| **Depends on** | [SPEC-150](SPEC-150_Session_State_Inspection.md) |
| **Related ADR** | [ADR-070](../adr/ADR-070_Session_State_Is_Inspectable.md) |

## Objective

Ensure every Session State field is inspectable through a single, uniform Session Inspection path.

## Design Principle

**Session Inspection is field-driven, not phrase-driven.**

Operators inspect Session State. They do not invoke business reasoning.

## Session State Registry

`SESSION_STATE_FIELDS` in `SessionStateManager.js` describes every inspectable field:

| Field | Aliases (examples) |
|---|---|
| Operating Mode | operating mode, mode, current mode, how are you operating |
| Execution Policy | execution policy, execution mode, are you allowed to execute, current execution policy |
| Reasoning Mode | reasoning mode, thinking mode, how are you reasoning, what reasoning mode are you using |
| Conversation Style | conversation style, response style, communication style, how are you responding |
| Evaluation Mode | evaluation mode, what are we evaluating, evaluation state, current evaluation |
| Session Summary | session state, current session, summarize session, session summary, how are you configured |

## Inspection Dispatch

```text
Operator Question
  → resolveSessionStateField()
  → getCurrentState()
  → formatSessionFieldInspection()
  → Response
```

Single-field questions return only that field. Summary aliases return the complete Session State block.

## Response Contract

**Single field**

```text
Current Session

Reasoning Mode

Natural
```

**Summary**

```text
Current Session

Operating Mode
…
Evaluation Mode
…
```

## Follow-up Reasoning

Inspection and explanation remain separate:

- *What execution policy are you using?* → Session Inspection
- *Why are you using that execution policy?* → Read Session State → Explain

## Files

| File | Role |
|---|---|
| `packages/max/workspace/SessionStateManager.js` | `SESSION_STATE_FIELDS`, `resolveSessionStateField`, `formatSessionFieldInspection` |
| `packages/max/workspace/SessionInspectionOperator.js` | Field-aware `inspectCurrentSession` |
| `packages/max/workspace/WorkspaceEngine.js` | Passes operator question into inspection operator |
| `packages/max/workspace/tests/spec150aSessionInspectionCoverage.test.js` | Acceptance tests |

## Acceptance Tests

1. What operating mode are you using? → Session Inspection (Operating Mode only)
2. What execution policy are you following? → Session Inspection (Execution Policy only)
3. What reasoning mode are you using? → Session Inspection (Reasoning Mode only)
4. What conversation style is active? → Session Inspection (Conversation Style only)
5. What evaluation mode is active? → Session Inspection (Evaluation Mode only)
6. Summarize your current session. → Complete Session Summary
7. Why are you using that reasoning mode? → Read Session State → Reasoning

None invoke Business Advisory, Mission Runtime, Scout, Ownership, or generic reasoning.

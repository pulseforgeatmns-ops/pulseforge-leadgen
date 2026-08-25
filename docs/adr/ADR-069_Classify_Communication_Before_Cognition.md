# ADR-069 — Classify Communication Before Cognition

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Related** | [SPEC-149](../specs/SPEC-149_Message_Type_Classification.md), [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md) |

## Context

The workspace runtime previously began with cognition — `classifyOperatorCognition()` and conversation subject detection ran before distinguishing whether the operator was configuring the session, asking a question, or issuing a command. That forced a single layer to answer two unrelated questions:

1. What kind of message is this?
2. What reasoning mode should be used?

Session directives such as *"For the remainder of this conversation… Don't execute anything. Explain your reasoning naturally."* were misrouted into reflection or explain pipelines because phrases like "explain your reasoning" matched cognition patterns.

## Decision

1. **Every operator message is classified by communicative purpose first** via `classifyMessageType()` in `MessageTypeClassifier.js`.
2. **Only after classification** shall reasoning, ownership, mission routing, or execution be evaluated.
3. **`SESSION_CONFIGURATION` bypasses cognition** — session state is updated and Max acknowledges; no reasoning pipeline runs.
4. **Ownership never processes configuration messages** — `resolveWorkspaceOwner()` returns early for `SESSION_CONFIGURATION`.
5. **Reasoning operators never classify communication purpose** — that responsibility belongs exclusively to the Message Type Classifier.

## Precedence

```text
Operator Message
  ↓
Message Type Classifier (SPEC-149)
  ↓
Session State Manager (if applicable)
  ↓
Conversation Contract
  ↓
Operator Intent / Cognition
  ↓
Ownership
  ↓
Reasoning
  ↓
Composition
```

> **ADR-087 (2026-08-25):** Within the Message Type Classifier, the operator's **primary business objective** is resolved before execution or conversation modifiers. Modifiers may mutate session state and influence presentation but must not displace routing. See [ADR-087](ADR-087_Operator_Objective_Takes_Precedence.md).

## Consequences

- Session configuration turns return acknowledgement only — no reflection bleed-through
- Questions and commands route through existing cognition and mission pipelines unchanged
- Routing traces include `messageType` for observability
- Conversation Subject Routing (legacy SPEC-149 subject layer) remains complementary — subject governs owner; message type governs whether cognition runs at all

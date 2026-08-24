# ADR-071 — Session Directive Registry

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-24 |
| **Related** | [SPEC-148](../specs/SPEC-148_Session_State_Manager.md), [SPEC-149](../specs/SPEC-149_Message_Type_Classification.md), [ADR-068](ADR-068_Session_State_Is_Explicit.md), [ADR-069](ADR-069_Classify_Communication_Before_Cognition.md) |

## Context

Session Configuration currently involves two independent responsibilities:

1. Classifying whether an operator message is configuring the current session.
2. Extracting which Session State fields should be mutated.

Historically these responsibilities evolved independently and maintained separate vocabularies. This allowed the classifier to recognize a Session Configuration message while the extractor failed to identify any writable fields. The result was:

```text
SESSION_CONFIGURATION
  ↓
Acknowledged
  ↓
No Session State mutations
```

even though the operator's intent was correctly understood.

## Decision

Session Configuration shall be driven by a **single Directive Registry**.

The Directive Registry becomes the canonical vocabulary describing operator directives. Both classification and extraction consume the same registry. The registry is the single source of truth.

### Architecture

```text
Operator Message
  ↓
Directive Registry
        ↓                  ↓
Classification      Field Extraction
        ↓                  ↓
SESSION_CONFIGURATION
  ↓
Session State Mutation
  ↓
Persistence
  ↓
Inspection
```

Classification and extraction no longer maintain separate linguistic vocabularies.

### Directive

Each directive defines:

```javascript
Directive {
  aliases;
  targetField;
  parsedValue;
  confidence;
}
```

Example:

```javascript
{
  aliases: [
    'use concise responses',
    'be concise',
    'respond concisely',
  ],
  targetField: 'conversationStyle',
  parsedValue: 'concise',
}
```

### Architectural Principles

1. **Single Vocabulary** — Classification and extraction shall recognize the same operator language.
2. **One Interpretation** — An operator directive shall never classify successfully while extracting zero writable mutations because of vocabulary drift.
3. **Data Over Logic** — Adding support for new operator phrasing should normally require updating the Directive Registry, not modifying classifier or extractor code.
4. **Shared Semantics** — Classification determines what kind of communication this is; extraction determines what Session State should change. Both answers must originate from the same semantic interpretation.

## Consequences

### Positive

- Eliminates classifier/extractor drift
- New operator phrasing becomes data, not code
- Session Configuration becomes easier to extend
- Reduces duplicated pattern maintenance
- Improves consistency across all Session State mutations

### Tradeoffs

- Introduces one shared registry that both systems depend on
- Directive definitions become a core runtime asset and require disciplined maintenance

### Runtime Guarantee

A directive recognized by Session Configuration shall always be interpretable by Session State mutation. Classification and extraction may produce different outputs. They shall never produce different understandings of the operator's language.

## Implementation

| Module | Role |
|---|---|
| `packages/max/workspace/SessionDirectiveRegistry.js` | Canonical directive vocabulary (ADR-071) |
| `packages/max/workspace/MessageTypeClassifier.js` | Classification consumes registry |
| `packages/max/workspace/SessionStateManager.js` | Extraction and mutation consume registry |

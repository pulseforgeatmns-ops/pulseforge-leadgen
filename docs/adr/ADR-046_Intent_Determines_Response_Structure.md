# ADR-046 — Intent Determines Response Structure

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-18 |
| **Spec** | [SPEC-109](../specs/SPEC-109_Intent_Bound_Response_Selection.md) |
| **Related** | [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md), [ADR-045](ADR-045_Evidence_Before_Reasoning.md), [ADR-032](ADR-032_Strategy_Before_Language.md) |

## Context

Max can retrieve evidence and reason over it, then still present every answer as advice. Retrieval questions (`What have we completed recently?`) were returning Blueprint acquisition recommendations. That collapses three distinct jobs — structure, content, and advice — into a single advisory response type.

ADR-045 already separates evidence collection from reasoning. This decision separates **reasoning from presentation**: Max may retrieve and reason internally, but the operator-facing shape is bound to classified intent.

## Decision

1. **Intent selects a response contract before retrieval or specialist delegation.**
2. **Contracts define required, optional, and forbidden sections.** Retrieval forbids unsolicited strategy. Summary may recommend last. Recommendation is primary only when asked. Challenge revises. Investigation does not answer from unsupported memory.
3. **Advice is not the default response type.** Recommendations appear only when the operator asked or the contract explicitly permits an optional recommendation. Answer first. Advise second.
4. **Reasoning and presentation remain separate.** The same retrieved evidence can fill different contracts without changing the evidence itself.

## Consequences

### Positive

- Retrieval questions stay retrievals
- Operator-facing intelligence can reuse one retrieval/reasoning path without sounding like a strategy engine on every turn
- CIE and specialists cannot swallow a retrieval or investigation as Blueprint advice

### Negative / tradeoffs

- Classification must distinguish summary vs retrieval vs recommendation; ambiguous wording still needs a contract
- Existing inventory phrasing (`What I can verify`) is preserved for SPEC-105 compatibility alongside newer contract headings

### Follow-ups

- [x] SPEC-109 implementation
- [ ] Workspace UI section rendering for contract headings

# SPEC-154 — Active Reasoning Context

**Status:** Implemented  
**Depends on:** SPEC-146, SPEC-150, SPEC-151, SPEC-152, SPEC-153

## Summary

Introduces **Active Reasoning Context (ARC)** — a session-scoped reasoning graph that tracks the current proposition chain across Max Ask turns. Follow-up questions (`Why?`, `How?`, `What if?`, `Why not?`, etc.) bind to the **primary claim** instead of restarting identity or advisory retrieval.

## Module

- `packages/max/workspace/ActiveReasoningContext.js` — ARC types, follow-up binding, delta computation, synthesis

## Integration

| Layer | Change |
|---|---|
| `OperatorIntent.js` | Applies ARC continuity after SPEC-150; exposes `activeReasoningContext`, `primaryClaim`, `arcFollowUp` |
| `IdentityReasoning.js` | Answers bound follow-ups from `synthesizeFromArc()` before concept-graph retrieval |
| `IdentityConversationContext.js` | Passes ARC into reasoning pipeline |
| `WorkspaceEngine.js` | Advances ARC after each turn via `advanceActiveReasoningContext()` |
| `ConversationSubject.js` | Routes `When would you disagree with me?` to identity |

## Session shape

```javascript
session.context.activeReasoningContext = {
  primaryClaim,
  supportingClaims,
  assumptions,
  openQuestions,
  conversationGoal,
  reasoningChain,
  confidence,
  createdAt,
  updatedAt,
  goal,
  subject,
}
```

## Resolved question binding

Bare follow-ups resolve to claim-scoped questions:

- `Why?` → `claim_why(<current-chain-node>)`
- `How?` → `claim_how(<node>)`
- Substantive questions (compare with named specialist, who decides, job-boundary questions) advance the graph via concept-graph reasoning instead of binding.

## Tests

`packages/max/workspace/tests/spec154ActiveReasoningContext.test.js`

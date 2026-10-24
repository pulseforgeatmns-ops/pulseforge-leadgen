# SPEC-002 — Max Reasoning Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.8.0 |
| **Priority** | Highest |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |

## Objective

Transform Max from a retrieval system into a deterministic reasoning engine capable of evaluating opportunities, synthesizing evidence, ranking recommendations, and explaining every conclusion.

The Reasoning Engine is the first true intelligence layer of Pulseforge.

- It must never hallucinate.
- It must never invent facts.
- Every recommendation must be traceable to evidence contained within the Knowledge Graph.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- `docs/vision/Product_Constitution.md`
- `docs/architecture/Agent_Architecture.md`
- `docs/architecture/Knowledge_Graph_Architecture.md`
- [ADR-001](../adr/ADR-001_Conversation_First.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-003](../adr/ADR-003_Human_Approval.md)
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)

## Product Philosophy

Max does not make decisions. Max constructs arguments.

Every recommendation should answer:

- Why this?
- Why now?
- Why not?
- How confident are we?
- What evidence supports this?
- What evidence contradicts it?

The operator remains in control.

## Problem

Max can summarize and score in shadow, but operators lack a first-class, graph-grounded reasoning loop that produces ranked, evidenced, contradiction-aware recommendations without inventing facts or calling an LLM.

## Scope

- Package `packages/max/` — reasoning, context, strategies, aggregation, recommendations, explanations, reports, tests
- `ReasoningContextBuilder` (immutable context from Knowledge Query Engine)
- Strategy Registry + seven initial strategies
- Weighted score aggregation with independent confidence
- Contradiction-first StrategyResults
- Recommendation Builder (structured data only)
- Explanation Engine + Reasoning Report
- Deterministic test suite

## Out of Scope

- LLM prompting / natural language summaries
- Autonomous execution / automatic outbound
- Dashboard UI / voice interaction
- Agent orchestration / runtime wiring
- Persisted recommendation lifecycle UI (future)

## Dependencies

- ✅ SPEC-000 Repository Foundation
- ✅ SPEC-001 Knowledge Layer / Persistent Store
- ✅ SPEC-001A Knowledge Foundation
- ✅ SPEC-001B Graph Synchronization
- ✅ SPEC-001C Knowledge Query Engine

## Architecture

```text
Operator
  ↓
Max
  ↓
Reasoning Engine
  ↓
Knowledge Query Engine  (via KnowledgeService query API)
  ↓
KnowledgeService
  ↓
Knowledge Graph
```

**Rules**

- No reasoning module may query repositories directly.
- All graph access occurs through the Knowledge Query Engine surface on `KnowledgeService`.
- Strategies never query the graph.
- Strategies never mutate context.
- Strategies never produce recommendations — only observations (`StrategyResult`).

## Package Layout

```text
packages/max/
  reasoning/
  context/
  strategies/
  aggregation/
  recommendations/
  explanations/
  reports/
  tests/
```

## Data Model

### ReasoningContext

Immutable snapshot: `company`, `people`, `interactions`, `claims`, `evidence`, `timeline`, `relatedCompanies`, `metrics`, `neighborEdges`.

### StrategyResult

`strategy`, `scoreDelta` (−100…100), `confidence` (0…100), `supportingEvidence`, `contradictingEvidence`, `claims`, `summary`.

### Recommendation

`id`, `subject`, `type`, `priority`, `score`, `confidence`, `recommendedAction`, `supportingSignals`, `opposingSignals`, `claims`, `evidence`, `reasoningSummary` (structured: whyThis / whyNow / whyNot / confidenceBasis).

### ReasoningReport

`context`, `strategyResults`, `normalizedScores`, `contradictions`, `recommendation`, `explanation`, `executionTime`, `performance`.

## Initial Strategies

| Strategy | Measures |
|---|---|
| Opportunity | Growth, hiring, expansion, new services |
| Engagement | Outreach history, replies, opens, time since contact |
| Relationship | Existing relationships, referrals, mutual contacts, previous work |
| Decision Maker | Contact quality, role certainty, DM identification |
| Overflow | Hiring velocity, vendor requests, service demand, operational strain |
| Technology | Software detected, platform maturity, automation fit |
| Risk | Negative responses, existing contracts, inactivity, low confidence |

## Aggregation Weights

| Strategy | Weight |
|---|---|
| Opportunity | 30% |
| Relationship | 20% |
| Engagement | 15% |
| Decision Maker | 10% |
| Technology | 10% |
| Overflow | 10% |
| Risk | 5% |

Normalize to 0–100. **Confidence is independent of score** and must never be combined into a single number.

## Query Requirements

Reasoning may use: `findCompanies`, `findPeople`, `findInteractions`, `findClaims`, `findEvidence`, `neighbors`, `timeline`, `path`, `related`, `explain`.

No repository access.

## Implementation Plan

1. Create `packages/max` + types
2. ReasoningContextBuilder
3. Strategy Registry + seven strategies
4. Score Aggregator
5. Recommendation Builder + Explanation Engine + Report
6. Deterministic tests
7. Docs / release v0.8.0 — agents remain unwired

## Migration Strategy

- Additive library only
- Existing Max briefing / shadow orchestration unchanged
- No server wiring in this release

## Testing

- Strategy tests (independent)
- Aggregation (weights, priority, confidence independence)
- Contradiction tests
- Determinism tests
- Recommendation snapshot tests
- Explanation chain tests
- Performance under target latency

```bash
npm run test:max
```

## Acceptance Criteria

- [x] ReasoningContextBuilder implemented
- [x] Strategy Registry implemented
- [x] Seven initial strategies implemented
- [x] Weighted aggregation operational
- [x] Independent confidence scoring
- [x] Contradicting evidence supported
- [x] Recommendation Builder operational
- [x] Explanation Engine operational
- [x] Reasoning Report generated
- [x] Complete deterministic test suite
- [x] Runtime agents remain unwired
- [x] Existing runtime behavior unchanged

## Future Work

- ✅ Temporal memory / recommendation history (SPEC-003 / v0.8.1)
- Persist recommendation lifecycle (`proposed` → accepted/rejected/shadow)
- Operator review UI / CLI evolution
- Wire Max agent to consume ReasoningEngine (shadow-first)
- Conversation-first surface (v1.0)
- Limited auto-apply for non-customer-visible tasks under new ADR

## Definition of Done

By completion of v0.8.0, Max is capable of producing structured, evidence-backed recommendations that are deterministic, explainable, confidence-aware, and fully auditable. Every recommendation exposes both supporting and contradicting evidence, remaining completely independent of any language model.

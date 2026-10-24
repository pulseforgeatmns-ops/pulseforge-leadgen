# SPEC-183 — Cognitive Explainability

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-181](SPEC-181_Evidence_Native_Execution.md), [SPEC-172](../packages/acquisition-mission/tests/spec172.test.js), [SPEC-173](../packages/acquisition-mission/tests/spec172.test.js) |
| **Supersedes** | Fragmented graph structures |
| **ADR** | [ADR-098](../adr/ADR-098_Cognitive_Explainability.md) |

## Objective

One explainability graph traces every Scout recommendation through the full cognitive chain. Max and operators can answer "why?" for any recommendation, provider assignment, or judgment.

## Problem

Explainability is fragmented across four structures:

| Structure | File | Scope |
|---|---|---|
| InvestigationGraph | `investigation/InvestigationGraph.js` | SPEC-142 evidence nodes |
| InvestigationTree | `investigation/InvestigationTree.js` | SPEC-158 branch lineage |
| MemoryGraph | `memory/MemoryGraph.js` | SPEC-143 market memory |
| Explainability (provenance) | `max/scoutAcquisition/Explainability.js` | SPEC-100 provenance chain |

AMO handoff via `ScoutDiscoveryArtifact` (SPEC-172) serializes a subset. No unified operator-facing graph exists.

## Decision

Introduce `ExplainabilityGraph` as the canonical cognitive trace:

```
Mission Objective
  ↓
Market Definition (node)
  ↓
Hypotheses[] (nodes with kind, status, rationale)
  ↓
Investigation Plan (node with tasks[])
  ↓
Evidence[] (nodes with provider, confidence, source)
  ↓
Understanding (node — synthesized evidence)
  ↓
Judgment (node — heuristics + sufficiency)
  ↓
Recommendation (node — final output)
```

## Module

| File | Role |
|---|---|
| `packages/scout/explainability/ExplainabilityGraph.js` | Canonical graph builder + serializer |
| `packages/scout/explainability/index.js` | Public exports |
| `adapters/ScoutDiscoveryArtifact.js` | Project graph to AMO boundary (SPEC-173) |

## API

```js
buildExplainabilityGraph(investigationState, plan, synthesis, judgment) → ExplainabilityGraph

serializeForOperator(graph) → string[]   // human-readable chain
serializeForAmo(graph) → DiscoveryPayload  // SPEC-172/173 boundary projection
traceRecommendation(graph, recommendationId) → CognitiveTrace
```

## Graph Node Schema

```js
interface ExplainabilityNode {
  id: string
  kind: 'objective' | 'market_definition' | 'hypothesis' | 'plan' | 'evidence' | 'understanding' | 'judgment' | 'recommendation'
  label: string
  rationale: string
  parentIds: string[]
  metadata: object
  timestamp: string
}
```

## Migration

1. Create `ExplainabilityGraph` composing nodes from InvestigationState, plan, synthesis, and judgment.
2. Wire into `DiscoveryPipeline` final stage (before artifact handoff).
3. `ScoutDiscoveryArtifact` reads from ExplainabilityGraph — not ad-hoc serialization.
4. Deprecate direct use of InvestigationGraph and InvestigationTree for operator-facing output (keep for internal branch tracking until Phase 4).

## Operator Queries

Max shall answer:

| Question | Graph Traversal |
|---|---|
| "Why are we searching LinkedIn?" | recommendation → plan.tasks → evidence requirement → provider assignment |
| "Why was this lead rejected?" | recommendation → judgment → understanding → evidence gaps |
| "What terminology did we test?" | market_definition → hypotheses[kind=terminology] |

## Acceptance Criteria

- [x] `ExplainabilityGraph` builds from DiscoveryPipeline output
- [x] Every recommendation node has traceable ancestors to mission objective
- [x] `ScoutDiscoveryArtifact` projects from ExplainabilityGraph
- [x] Operator serialization produces human-readable cognitive chain
- [x] No recommendation traces directly to a provider without intermediate hypothesis/evidence nodes

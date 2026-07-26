# SPEC-001A — Knowledge Layer Foundation

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.7.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-26 |
| **Depends on** | [SPEC-000](SPEC-000_Repository_Foundation.md) |

## Objective

Create the abstraction layer that every future intelligence feature uses. No existing feature should know whether knowledge is stored in Postgres, Neo4j, Memgraph, Dgraph, or an in-memory graph. Everything communicates through one service: `KnowledgeService`.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- `docs/architecture/Knowledge_Graph_Architecture.md`
- `docs/architecture/Memory_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)

## Problem

SPEC-001 described a Business Knowledge Graph, but without a storage-agnostic service boundary the first persistence choice would leak into Scout, Max, and CRM code. We need domain objects, evidence/claims, explainability, and event-driven ingestion proven before picking a durable store.

## Scope

- New package `packages/knowledge/`
- `KnowledgeService` as the only public graph API
- `GraphRepository` storage contract
- `InMemoryGraphRepository` only (no Neo4j/Memgraph/Postgres-backed repo)
- `EvidenceEngine` and `ClaimEngine`
- Strongly typed node and edge domain objects
- `knowledge.explain(tenantId, nodeId)` chain
- Event bus + ingestor (Scout/CRM/Max must not write the repository directly)
- Unit tests for contract, tenancy, confidence, claims, explain, events

## Out of Scope

- UI / visual graph explorer
- LLM integration
- Recommendation engine
- Embeddings
- Graph database selection / persistent repository
- Production synchronization
- Wiring Scout/Max/CRM producers into the live server runtime

## Dependencies

- SPEC-000 complete
- Node >= 18 (`structuredClone`, `node:test`)

## Architecture

```text
Producers (future)     packages/knowledge
Scout / CRM / Max  →  KnowledgeEventBus → KnowledgeIngestor
                                              ↓
                                       KnowledgeService
                                       EvidenceEngine / ClaimEngine
                                              ↓
                                       GraphRepository
                                              ↓
                                   InMemoryGraphRepository (001A)
```

## Data Model

**Nodes:** Company, Person, Interaction, Evidence, Claim — each with `id`, `tenantId`, `createdAt`, `updatedAt`, `metadata`.

**Edges (enum):** `HAS_CONTACT`, `PARTICIPATED_IN`, `GENERATED`, `SUPPORTS`, `ABOUT`, `USES`, `LOCATED_IN`, `KNOWS`, `WORKS_FOR`.

## Implementation Plan

1. Create package skeleton and types
2. Implement domain nodes + edge constants
3. Implement repository contract + in-memory backend
4. Implement EvidenceEngine, ClaimEngine, KnowledgeService
5. Implement event bus + ingestor
6. Tests + docs (this spec, CURRENT_STATE, CHANGELOG, v0.7.1)

## Migration Strategy

None — additive package only. No schema migrations. No runtime wiring.

## Testing

```bash
npm run test:knowledge
# or
node --test packages/knowledge/tests/*.test.js
```

## Acceptance Criteria

- [x] `KnowledgeService` is the only public API for graph operations
- [x] Graph concepts are strongly typed domain objects
- [x] Evidence and claim models implemented
- [x] Event-driven ingestion in place
- [x] `InMemoryGraphRepository` satisfies repository contract
- [x] Existing runtime behavior unchanged (no server/agent wiring)
- [x] Unit tests cover creation, tenancy, confidence, claims, explain, events, contract

## Future Work

- Persistent `GraphRepository` implementation (SPEC-001 continuation / v0.8.0)
- Wire Scout/CRM events in shadow mode
- SPEC-002 Max Reasoning Engine consuming `explain()` / search

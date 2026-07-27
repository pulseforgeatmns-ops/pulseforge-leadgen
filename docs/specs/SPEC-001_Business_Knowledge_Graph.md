## Status

| Field | Value |
|---|---|
| **Status** | Partially superseded — persistent store delivered as [SPEC-001 Persistent Knowledge Store](SPEC-001_Persistent_Knowledge_Store.md) (v0.7.3) |
| **Remaining** | Shadow dual-write / production ingest wiring (unversioned; follows v0.8.0 reasoning library) |
| **Target Version** | TBD (remaining work; v0.8.0 reassigned to SPEC-002) |

## Objective

Introduce a client-scoped Business Knowledge Graph that stores entities, relationships, and provenance so Max and operators can query durable business memory—not just mutable CRM rows and chat context.

**Completed in v0.7.1–v0.7.3:** KnowledgeService abstraction, sync engine, Postgres persistent repository.

**Still open:** wiring Scout/CRM producers to emit sync events in shadow mode; production rebuild ops.

## Vision References

- `docs/vision/Product_Thesis.md`
- `docs/vision/Intelligence_Architecture.md`
- `docs/architecture/Knowledge_Graph_Architecture.md`
- `docs/architecture/Memory_Architecture.md`
- [ADR-004](../adr/ADR-004_Knowledge_Graph.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)

## Problem

Business context is fragmented across `prospects`, `companies`, `touchpoints`, `agent_log`, inquiry events, and Max signal tables. There is no first-class graph with provenance for “why is this warm?” or conversation retrieval.

## Scope

- Decide storage approach (Postgres graph tables vs derived projection—see architecture doc)
- Define core node/edge types for client, company, person, inquiry/opportunity, interaction
- Ingest adapters from existing canonical events (Scout insert, setter visibility, Brevo events, inquiry events, bookings where wired)
- Read API / query helpers for neighborhood + explanation packs
- Migration/backfill strategy with idempotency
- Documentation + tests

## Out of Scope

- Max full reasoning product surface (SPEC-002)
- Autonomous edge extraction from arbitrary LLM text without validators
- Replacing setter/closer UIs
- Cross-client queries
- Non-shadow outbound side effects

## Dependencies

- SPEC-000 complete
- [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md) complete (`KnowledgeService` + in-memory repository)
- [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md) complete (CRM→knowledge sync + rebuild)
- ADR-004 accepted
- Existing event sources documented in `docs/max-canonical-source-assessment.md`

## Architecture

Follow `docs/architecture/Knowledge_Graph_Architecture.md`. **Implement a persistent `GraphRepository`** behind the existing SPEC-001A `KnowledgeService` API — do not introduce a parallel graph client. Prefer boring Postgres. CRM tables remain system of record for mutations during v0.8 unless this spec explicitly promotes KG writes for a narrow path.

## Data Model

TBD in implementation spike — must include:

- `client_id` on all nodes/edges
- provenance references to source events
- observed-at timestamps
- unique idempotency keys per source event → edge/claim

## Implementation Plan

1. Spike storage choice + ADR amendment if needed
2. Schema migration
3. Ingest adapters (read-only consumers first)
4. Backfill job (shadow, resumable)
5. Query helpers + tests
6. Operator/docs update; hand off to SPEC-002

## Migration Strategy

- Additive schema
- Resumable backfill
- No destruction of CRM rows
- Rollback = stop ingest + optional table drop via rollback SQL (manual)

## Testing

- Unit tests for idempotent ingest
- Postgres integration for graph constraints
- Fixture client isolation tests

## Acceptance Criteria

- [ ] Documented node/edge model with tenancy + provenance
- [ ] At least three live event types ingest idempotently
- [ ] Query helper returns neighborhood for a prospect with evidence refs
- [ ] Backfill can run safely without outbound side effects
- [ ] CURRENT_STATE / CHANGELOG updated when remaining ingest ships

## Future Work

- SPEC-002 Max Reasoning Engine
- Additional event types (meeting cancelled/showed once canonical sources exist)
- Conversation retrieval APIs

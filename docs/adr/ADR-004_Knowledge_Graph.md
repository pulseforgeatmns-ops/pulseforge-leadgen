# ADR-004 — Knowledge Graph

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-26 |
| **Spec** | SPEC-001 |
| **Supersedes** | — |

## Context

CRM tables and logs are necessary but insufficient as product memory. Conversation-first and Max reasoning require traversable entities/relationships with provenance. Alternatives considered: (a) prompt-stuffing recent SQL rows, (b) vector-only memory, (c) a formal knowledge graph.

## Decision

Pulseforge will implement a **Business Knowledge Graph** as the durable memory layer for intelligence features (SPEC-001). Storage should prefer **Postgres-hosted** graph/claim tables or an explicit derived projection—not a separate exotic database—unless SPEC-001 proves a hard need otherwise.

The graph must be:

- Client-scoped
- Provenance-bearing
- Idempotent under re-ingest
- Compatible with existing CRM tables during migration

Vector search may complement the graph later; it does not replace structured relationships.

## Consequences

### Positive

- Shared substrate for Max, conversation, and explainability
- Clear migration path from event sources already assessed

### Negative / tradeoffs

- Up-front modeling cost
- Risk of dual-write complexity if not designed as projection-first

### Follow-ups

- SPEC-001 chooses concrete schema and ingest adapters
- SPEC-002 consumes graph reads for reasoning

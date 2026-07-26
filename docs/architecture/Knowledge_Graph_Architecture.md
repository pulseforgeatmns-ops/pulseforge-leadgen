# Knowledge Graph Architecture

**Status:** Accepted direction ([ADR-004](../adr/ADR-004_Knowledge_Graph.md)).

**Implementation status:**

- [SPEC-001A](../specs/SPEC-001A_Knowledge_Layer_Foundation.md) (v0.7.1) — **Done**: storage-agnostic `packages/knowledge` with in-memory repository, evidence/claims, events, `explain()`.
- [SPEC-001B](../specs/SPEC-001B_Graph_Synchronization_Engine.md) (v0.7.2) — **Done**: idempotent CRM→`KnowledgeService` sync + relational rebuild (no direct repository access).
- [SPEC-001](../specs/SPEC-001_Persistent_Knowledge_Store.md) (v0.7.3) — **Done**: Postgres `PersistentGraphRepository` behind the same interface; `KnowledgeService` unchanged.
- [SPEC-001C](../specs/SPEC-001C_Knowledge_Query_Engine.md) (v0.7.4) — **Done**: typed Query Engine on `KnowledgeService` (filters, traversal, timeline, path, metrics).
- Remaining production ingest / shadow dual-write — see [SPEC-001_Business_Knowledge_Graph.md](../specs/SPEC-001_Business_Knowledge_Graph.md) (draft).

## Purpose

Provide a single, queryable memory of business reality: who entities are, how they relate, what happened, and why we believe it.

## Conceptual model

```text
(Client)-owns->(Company)-employs->(Person/Prospect)
     |                |
     |                +--participates_in-->(Inquiry|Opportunity)
     |
     +--has_event-->(Interaction: email|call|meeting|content)
                          |
                          +--evidenced_by-->(RawEvent / touchpoint / webhook)
```

## Required properties

| Property | Requirement |
|---|---|
| Tenancy | Every node/edge scoped to `client_id` |
| Provenance | Edges/claims cite source event IDs |
| Time | Valid-from / observed-at where relevant |
| Idempotency | Re-ingesting the same source event does not duplicate edges |
| Explainability | Max can walk evidence for a recommendation |
| Compatibility | Existing CRM tables remain readable during migration |

## Storage

**Implemented (v0.7.3):** Postgres `PersistentGraphRepository` with graph-owned tables. Application code continues to use `KnowledgeService` / `GraphRepository` only.

Future options if Postgres proves insufficient:

1. Dedicated graph database implementing the same `GraphRepository` interface
2. Hybrid projection models

Swapping storage must not change `KnowledgeService`.

## Non-goals (v0.8)

- Replacing setter UI in one leap
- Cross-client graph queries
- Automated edge creation from unconstrained LLM extraction without validation

## Interface sketch (future)

- Ingest adapters: Scout insert, Brevo webhook, inquiry events, Cal booking
- Query API: neighborhood by prospect/company; “why warm?” explanation pack
- Max tool surface: constrained graph reads only (no silent writes from the LLM)

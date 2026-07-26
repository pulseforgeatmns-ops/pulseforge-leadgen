# Knowledge Graph Architecture

**Status:** Accepted direction ([ADR-004](../adr/ADR-004_Knowledge_Graph.md)).

**Implementation status:**

- [SPEC-001A](../specs/SPEC-001A_Knowledge_Layer_Foundation.md) (v0.7.1) — **Done**: storage-agnostic `packages/knowledge` with in-memory repository, evidence/claims, events, `explain()`.
- [SPEC-001](../specs/SPEC-001_Business_Knowledge_Graph.md) (v0.8.0) — persistent repository + production-oriented ingest (not started).

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

## Storage options (to decide in SPEC-001)

Candidates (non-final). **Application code must keep using `KnowledgeService` / `GraphRepository` only** — see SPEC-001A.

1. Postgres relational graph tables (`kg_nodes`, `kg_edges`, `kg_claims`) implementing `GraphRepository`
2. JSONB document projections beside relational CRM
3. Hybrid: CRM remains SoR for mutations; KG is a derived projection rebuilt from events

SPEC-001 must pick one with migration and rollback strategy. Prefer boring Postgres unless a measured need appears. Swapping `InMemoryGraphRepository` for a persistent implementation must not change callers.

## Non-goals (v0.8)

- Replacing setter UI in one leap
- Cross-client graph queries
- Automated edge creation from unconstrained LLM extraction without validation

## Interface sketch (future)

- Ingest adapters: Scout insert, Brevo webhook, inquiry events, Cal booking
- Query API: neighborhood by prospect/company; “why warm?” explanation pack
- Max tool surface: constrained graph reads only (no silent writes from the LLM)

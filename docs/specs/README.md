# Specs

Implementation contracts for Pulseforge. Specs are the bridge between vision and code.

## Active and planned

| Spec | Title | Status | Release |
|---|---|---|---|
| [SPEC-000](SPEC-000_Repository_Foundation.md) | Repository Foundation & Source of Truth | Done | v0.7.0 |
| [SPEC-001A](SPEC-001A_Knowledge_Layer_Foundation.md) | Knowledge Layer Foundation | Done | v0.7.1 |
| [SPEC-001B](SPEC-001B_Graph_Synchronization_Engine.md) | Graph Synchronization Engine | Done | v0.7.2 |
| [SPEC-001](SPEC-001_Persistent_Knowledge_Store.md) | Persistent Knowledge Store | Done | v0.7.3 |
| [SPEC-001_Business_Knowledge_Graph.md](SPEC-001_Business_Knowledge_Graph.md) | Business Knowledge Graph (remaining production ingest) | Draft — next | v0.8.0 |
| [SPEC-002](SPEC-002_Max_Reasoning_Engine.md) | Max Reasoning Engine | Draft — queued | v0.9.0 |

## Process

1. Copy [TEMPLATE.md](TEMPLATE.md).
2. Number sequentially.
3. Link vision docs and ADRs under Vision References.
4. Set status in the spec header and in `CURRENT_STATE.md` when work starts.
5. Implement only what Scope allows.
6. Close when Acceptance Criteria pass; update CHANGELOG + CURRENT_STATE.

## Rules

- Specs do not redefine Product Constitution — they implement it.
- Architecture changes require an ADR.
- Every PR links its spec.
- Prefer slicing a large spec into sequenced PRs over silent scope creep.

## Template sections (required)

Objective · Vision References · Problem · Scope · Out of Scope · Dependencies · Architecture · Data Model · Implementation Plan · Migration Strategy · Testing · Acceptance Criteria · Future Work

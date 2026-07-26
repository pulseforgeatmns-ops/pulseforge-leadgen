# Product Roadmap

High-level capability roadmap. Release detail: `docs/releases/`. Live status: `CURRENT_STATE.md`.

## v0.7.0 — Foundation

- Repository as source of truth
- Vision / architecture / specs / ADR / release hierarchy
- Contributor onboarding for humans and AI

## v0.7.1 — Knowledge Layer Foundation

- `packages/knowledge` — `KnowledgeService`, evidence/claims, events, `explain()`
- In-memory repository only; no runtime wiring

## v0.7.2 — Graph Synchronization Engine

- `GraphSyncEngine` — idempotent CRM/import/rebuild → KnowledgeService
- Still library-only (no production dual-write)

## v0.7.3 — Persistent Knowledge Store

- Postgres `PersistentGraphRepository` behind the same `GraphRepository` interface
- `KnowledgeService` unchanged
- Still no agent/server dual-write

## v0.8.0 — Business Knowledge Graph (production ingest)

- Shadow event ingest from Scout/CRM
- Read APIs / projection suitable for Max and operators
- Operational rebuild runbooks

## v0.9.0 — Max Reasoning Engine

- SPEC-002: reasoning over KG + evidence
- Explainable recommendations (ADR-002)
- Still shadow-first for mutating actions

## v1.0 — Conversation-first operating partner

- Conversation surfaces over KG + Max
- Mature approval and outbox paths for authorized tenants
- Production-ready inquiry + outreach loops where clients are configured

## Parallel operational tracks (not version-gated alone)

These continue alongside the product version line and must not silently redefine roadmap:

- Inquiry Foundation production authorization
- Max orchestration flag graduation (shadow → limited write)
- Anchor Cleaning Scout / setter revenue loops
- Revenue projection certification and rebuild tooling
- Multi-client Scout markets (NH, WV, Nashville)

## Explicitly later

- Fully autonomous multi-channel campaigns
- Client-facing white-label chat without operator oversight
- Replacing setter/closer human roles

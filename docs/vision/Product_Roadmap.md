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

## v0.8.0 — Max Reasoning Engine

- SPEC-002: deterministic strategies, weighted score, independent confidence, contradictions, explanations
- Library only (`packages/max`); agents unwired

## v0.8.1 — Temporal Intelligence & Memory

- SPEC-003: append-only reasoning snapshots, diffs, change detection, trends, watches (detection only)
- Memory queries (`whatChanged`, `whyChanged`, `history`, …); agents still unwired

## v0.9.0 — Max Briefing Engine

- SPEC-004: assembles Knowledge + Reasoning + Memory into deterministic operator briefings
- `max.brief({ tenantId, asOf, period })`; Presentation Adapter extension point
- Library only; agents unwired

## v0.9.1 — Policy & Decision Engine

- SPEC-005: deterministic policy evaluation (allow / warn / requireApproval / block)
- Seven initial rules; per-tenant data-driven config; immutable audit
- Library only; no automatic execution; agents unwired

## v1.0.0 — Pulseforge Command Deck

- SPEC-006: intelligence-first operator surface (Morning Brief, HLA, Priority Queue, Ask Max)
- Consumes Briefing + Reasoning + Memory + Policy — never recreates them
- Constitution §11 Cognitive load

## Parallel — Operator wiring / graph ingest

- Optional Max agent→`brief()` + `decide()` shadow wiring
- Shadow dual-write Scout/CRM → GraphSyncEngine
- Production rebuild ops / runbooks

## Remaining — Business Knowledge Graph (production ingest)

- Shadow dual-write Scout/CRM → GraphSyncEngine
- Production rebuild ops
- Operational rebuild runbooks

## v1.0 — Intelligence-first operating partner

- Command Deck as primary landing experience
- Contextual Ask Max over KG + Max
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

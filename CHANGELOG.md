# Changelog

All notable changes to this project are documented here. Format inspired by [Keep a Changelog](https://keepachangelog.com/). Versions follow the release plans in `docs/releases/`.

## [Unreleased]

### Planned

- Shadow CRM/Scout → GraphSyncEngine dual-write
- SPEC-002 Max Reasoning Engine

## [0.7.3] — 2026-07-26

### Added

- Persistent knowledge store ([SPEC-001](docs/specs/SPEC-001_Persistent_Knowledge_Store.md))
  - Postgres tables `knowledge_nodes`, `knowledge_edges`, `knowledge_evidence`, `knowledge_claims`
  - `PersistentGraphRepository` implementing the existing `GraphRepository` contract
  - Migration `2026-07-26-knowledge-graph-persistent.sql`
  - Postgres tests via `npm run test:knowledge:postgres`

### Notes

- `KnowledgeService` public API unchanged (hash-guarded in tests)
- Default runtime remains in-memory unless a persistent repository is injected
- Agents/server remain unwired

## [0.7.2] — 2026-07-26

### Added

- Graph synchronization engine ([SPEC-001B](docs/specs/SPEC-001B_Graph_Synchronization_Engine.md))
  - `GraphSyncEngine` with idempotent `apply` / `applyMany` / `rebuildFromRelational`
  - CRM mappers for companies, prospects, touchpoints, import batch items
  - `InMemorySyncLedger` + `MemoryRelationalSource` + read-only `PostgresRelationalSource`
  - `KnowledgeService.ensureNode` / `ensureEdge` and `EvidenceEngine.ensureEvidence`
  - Idempotent `KnowledgeIngestor` (stable evidence IDs)

### Notes

- No server/agent wiring — production runtime unchanged

## [0.7.1] — 2026-07-26

### Added

- Knowledge layer foundation ([SPEC-001A](docs/specs/SPEC-001A_Knowledge_Layer_Foundation.md))
  - Package `packages/knowledge/` with `KnowledgeService` as the only public graph API
  - `GraphRepository` contract + `InMemoryGraphRepository`
  - `EvidenceEngine`, `ClaimEngine`, confidence helpers
  - Event bus + ingestor (`KnowledgeEventBus`, `KnowledgeIngestor`)
  - `explain()` chain: Claim → Evidence → Original Source → Confidence → Reason
  - Unit tests via `npm run test:knowledge`

### Notes

- No runtime wiring — existing agents/server behavior unchanged
- No persistent graph store yet (deferred to SPEC-001)

## [0.7.0] — 2026-07-26

### Added

- Repository foundation as source of truth ([SPEC-000](docs/specs/SPEC-000_Repository_Foundation.md))
  - Root: `README.md`, `CONTRIBUTING.md`, `PROJECT_CONTEXT.md`, `CURRENT_STATE.md`, `DECISIONS.md`, `CHANGELOG.md`
  - `docs/00_START_HERE.md`
  - Vision suite under `docs/vision/`
  - Architecture suite under `docs/architecture/`
  - Spec templates + SPEC-000/001/002 under `docs/specs/`
  - ADR templates + ADR-001–004 under `docs/adr/`
  - Release plans `v0.7.0` → `v1.0` under `docs/releases/`

### Notes

- Does not change runtime behavior, schema, or production flags.
- Existing flat runbooks under `docs/*.md` remain valid operational references.

## Pre-0.7.0 (summary)

Prior work lived without a versioned product changelog. Notable engineering streams already in-tree:

- Multi-agent lead-gen CRM (Scout, Emmett, Riley, Max, social, setter/closer)
- Max prospect orchestration Phase 1–2.5 (shadow-default)
- Inquiry Foundation, work queue, outbound outbox, Operator Command Center (local/shadow)
- Anchor Cleaning buyer Scout + verified queue / phone setter tooling
- Revenue projection Phase 15–16B tooling and certification docs

See individual files under `docs/` for stream-specific history.

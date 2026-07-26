# Changelog

All notable changes to this project are documented here. Format inspired by [Keep a Changelog](https://keepachangelog.com/). Versions follow the release plans in `docs/releases/`.

## [Unreleased]

### Added

- Command Deck UI ([SPEC-008](docs/specs/SPEC-008_Command_Deck_UI.md)) — render-only surface
  - `GET /command-deck` — Morning Brief, Highest Leverage Action, Intelligence Cards, Priority Queue, Ask Max launcher
  - Consumes only `CommandDeckModel` from `GET /api/v1/command-deck`
  - Staged reveal, composer empty states, calm error + last-successful recovery
  - Shell nav link for admin / manager / viewer / client; `/dashboard` unchanged

### Planned

- Pulseforge Command Deck remaining experience ([SPEC-006](docs/specs/SPEC-006_Command_Deck.md)) — Ask Max workspace, Recommendation Detail, Company Intelligence
- Shadow CRM/Scout → GraphSyncEngine dual-write
- Wire Max agent (shadow) to `brief()` + `decide()` + `compose()` before side effects

### Docs

- SPEC-008 Command Deck UI approved and indexed
- SPEC-006 Command Deck product surface remains the parent v1.0 experience spec
- Product Constitution §11 Cognitive load
- Roadmap / CURRENT_STATE / Product Experience / v1.0 release plan aligned to Command Deck

## [0.9.2] — 2026-07-26

### Added

- Command Deck Composition Engine ([SPEC-007](docs/specs/SPEC-007_Command_Deck_Composition_Engine.md))
  - `packages/max/commandDeck/` — CommandDeckComposer, IntelligenceCard contract, empty states
  - `max.compose({ tenantId, asOf, period })` → immutable `CommandDeckModel`
  - Assembles Morning Brief, Highest Leverage Action, Watch Alerts, Market Trends, Priority Queue
  - Explainability metadata on every card; composer-owned empty states
  - `GET /api/v1/command-deck` — one API, one payload, render-only UI contract

### Notes

- Reasoning / Memory / Briefing / Policy cores unchanged
- Enables SPEC-006 Command Deck UI without dashboard-side intelligence orchestration

## [0.9.1] — 2026-07-26

### Added

- Policy & Decision Engine ([SPEC-005](docs/specs/SPEC-005_Policy_Decision_Engine.md))
  - `packages/max/policy/` — PolicyEngine, RuleRegistry, seven initial rules
  - `policy.evaluate({ tenantId, recommendation, context })` / `max.decide(...)`
  - Data-driven per-tenant policy; immutable audit trail; explainability chain
  - Outcomes: allow, warn, requireApproval, block — evaluation only (no execution)

### Notes

- Reasoning / Memory / Briefing cores unchanged; runtime agents remain unwired

## [0.9.0] — 2026-07-26

### Added

- Max Briefing Engine ([SPEC-004](docs/specs/SPEC-004_Max_Briefing_Engine.md))
  - `packages/max/briefing/` — assembles Knowledge + Reasoning + Memory into structured briefings
  - `max.brief({ tenantId, asOf, period })` — daily / weekly / monthly digests
  - Sections: summary, priorities, changes, watchAlerts, risks, recommendations, metrics
  - Deterministic prioritization; Presentation Adapter extension point (structured + markdown)
  - Briefing never calls `evaluate()` — assembles existing intelligence only

### Notes

- Reasoning + Memory cores unchanged; runtime agents remain unwired
- Default output is domain objects only (no UI formatting)

## [0.8.1] — 2026-07-26

### Added

- Temporal Intelligence & Memory ([SPEC-003](docs/specs/SPEC-003_Temporal_Intelligence_Memory.md))
  - `packages/max/memory/` — append-only snapshots, deterministic diffs, change detection
  - Timeline history, recommendation evolution (trend + linear forecast), temporal explanations
  - Memory queries: `whatChanged`, `whyChanged`, `history`, `trend`, `scoreHistory`, `confidenceHistory`
  - Watch registration (detection only — no notifications)
  - Repository parity: InMemory + Serializing snapshot stores

### Notes

- Reasoning core (SPEC-002) unchanged; runtime agents remain unwired
- Snapshots are structured state only — no LLM output

## [0.8.0] — 2026-07-26

### Added

- Max Reasoning Engine ([SPEC-002](docs/specs/SPEC-002_Max_Reasoning_Engine.md))
  - Package `packages/max/` — ReasoningContextBuilder, Strategy Registry, seven strategies
  - Weighted ScoreAggregator with independent confidence (never mixed into score)
  - RecommendationBuilder, ExplanationEngine, ReasoningReport (no LLM)
  - Deterministic tests via `npm run test:max`

### Notes

- Graph access only through KnowledgeService query API — no repository access from reasoning
- Runtime agents/server remain unwired; existing Max briefing behavior unchanged
- Score and confidence are separate; contradictions are first-class on every strategy

## [0.7.4] — 2026-07-26

### Added

- Knowledge Query Engine ([SPEC-001C](docs/specs/SPEC-001C_Knowledge_Query_Engine.md))
  - `packages/knowledge/query/` — QueryEngine, Filters, Traversal, Timeline, Metrics
  - KnowledgeService query API: `findCompanies`, `findPeople`, `findInteractions`, `neighbors`, `related`, `timeline`, `path`
  - Enhanced `explain()` with timeline position (Claim → Evidence → Source → Confidence → Timeline → Reason)
  - Structured per-query metrics (`queryName`, timing, nodes/edges, repository type)
  - In-memory + Postgres repository parity tests

### Notes

- Legacy `(tenantId, …)` signatures for `findEvidence` / `findClaims` / `explain` preserved
- No GraphRepository contract changes; agents/server remain unwired
- Numbered SPEC-001C to avoid colliding with draft SPEC-002 (Max Reasoning Engine)

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

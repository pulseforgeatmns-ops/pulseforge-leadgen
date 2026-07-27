# Changelog

All notable changes to this project are documented here. Format inspired by [Keep a Changelog](https://keepachangelog.com/). Versions follow the release plans in `docs/releases/`.

## [Unreleased]

### Added

- Opportunity Ranking Capability ([SPEC-026](docs/specs/SPEC-026_Opportunity_Ranking_Capability.md))
  - `packages/capabilities/ranking/` — explainable 8-factor scoring, Opportunity Briefs, review package
  - Replaces Opportunity Ranking stub; answers “Who should we contact first?”
  - Operator actions: approve / re-rank / exclude / lock / continue to Campaign Builder
  - Evidence-only (absent signals score 0 — no invented buying signals)
  - Tests: `npm run test:capabilities`
- Prospect Discovery Capability ([SPEC-024](docs/specs/SPEC-024_Prospect_Discovery_Capability.md))
  - Profile-driven discovery (Places + fixture provider), transparent ranking signals, review package
- Mission Engine thin slice ([SPEC-022](docs/specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) / [ADR-010](docs/adr/ADR-010_Mission_Engine.md))
  - `packages/mission-engine/` — IntentRouter, MissionPlanner, MissionExecutor, durable store
  - `packages/capabilities/` — CapabilityRegistry, CapabilityRunner; Discovery + Ranking live, enrichment/knowledge/campaign stubs ([SPEC-023](docs/specs/SPEC-023_Capability_Framework.md) / [ADR-011](docs/adr/ADR-011_Capability_Framework.md))
  - Mission-first Max routing: “Build Campaign 001” → MissionPlanner (not Market Intelligence)
  - Command Deck Operations (Mission Queue) beneath Highest Leverage Action; Mission Workspace
  - Shell Operations nav redirects to `/command-deck#operations`
  - API: `POST/GET /api/v1/missions`, `GET /api/v1/missions/:id`, `POST /api/v1/missions/:id/review`
  - Flag: `MISSION_ENGINE` default on; set `=0` to disable
  - Tests: `npm run test:mission` · `npm run test:capabilities`
- Learning & Belief Evolution Engine ([SPEC-021](docs/specs/SPEC-021_Learning_and_Belief_Evolution_Engine.md))
  - `packages/learning/` — LearningEngine, BeliefTracker, CalibrationEngine, OutcomeEvaluator, LearningSession
  - Outcomes calibrate trust after reality is known (no ML / no history·replay·runtime mutation)
  - EQL: `SHOW Calibration FOR Claim("…")`, `SHOW Accuracy FOR StrategyPack("…")`
  - Laboratory: `lab.compareCalibration(...)`, `lab.replayWithCalibration(...)`
- Evidence Query Language ([SPEC-020](docs/specs/SPEC-020_Evidence_Query_Language.md))
  - `packages/eql/` — Parser, QueryPlanner, Executor, EvidenceCatalog
  - Declarative FIND / SHOW / REPLAY / COMPARE / EXPLAIN (read-only)
  - Domain-neutral subject matching (`subject` / `subjectId` / `companyId`)
  - `lab.query(\`…\`)` on Evidence Laboratory
- Evidence Laboratory ([SPEC-019](docs/specs/SPEC-019_Evidence_Laboratory.md))
  - `packages/laboratory/` — EvidenceLab, ScenarioRunner, EvidenceQuery, ComparisonWorkspace, Experiment
  - Isolated experiments for ablations, injections, strategy/ontology comparison, analogs
  - Asks questions of the Evidence Platform via Replay; nothing mutates production (not paper trading)
- Deterministic Replay & Temporal Reasoning Engine ([SPEC-018](docs/specs/SPEC-018_Deterministic_Replay_and_Temporal_Reasoning_Engine.md))
  - `packages/replay/` — ReplayEngine, ReplaySession, ReplayTimeline, ReplayComparator
  - Regenerates reasoning from immutable observations (no snapshots / cached conclusions)
  - Temporal queries: belief at T, confidence rises, recommendation changes, claim appearance
  - Version-aware comparison across ontology / strategy pack / runtime
  - Determinism hardening: market recommendation ids, context `builtAt`, fixed analog timestamps; runtime injectable clock
- Evidence Platform Architecture ([ADR-009](docs/adr/ADR-009_Evidence_Platform_Architecture.md))
  - Formalizes domain-neutral Evidence Core vs Strategy Pack boundary
  - Stable primitives: Evidence Store, KG, Claim/Confidence engines, Memory, Reasoning Runtime
  - Audit: [EVIDENCE_CORE_DOMAIN_AUDIT.md](docs/architecture/EVIDENCE_CORE_DOMAIN_AUDIT.md)
- Reasoning Runtime Decoupling ([SPEC-015A](docs/specs/SPEC-015A_Reasoning_Runtime_Decoupling.md))
  - `packages/reasoning-runtime/` — domain-neutral `ReasoningRuntime` + StrategyPack / ContextProvider / RecommendationProvider contracts
  - `CRMStrategyPack` + `CRMContextProvider` + `NextBestActionProvider` (DI wrappers; identical CRM evaluate path)
  - Max `ReasoningEngine` orchestrates via runtime; `subjectId` alias for `companyId`; `createCRMStrategyRegistry` alias
  - Architecture: [Reasoning_Runtime_Architecture.md](docs/architecture/Reasoning_Runtime_Architecture.md)
- Knowledge Dual-Write & Operational Readiness ([SPEC-014](docs/specs/SPEC-014_Knowledge_Dual_Write.md))
  - Outbox-first Knowledge writer (`packages/knowledge/dualWrite/`) with idempotent sync ledger
  - Durable tables: `knowledge_outbox`, `knowledge_sync_ledger`, `knowledge_flight_stages`
  - Producer hooks: `dbClient` company/prospect/touchpoint + Scout fan-out (alongside Max signals)
  - Persistent Knowledge boot for Command Deck / Max (`utils/knowledgeRuntime.js`)
  - Admin Validation Dashboard (`/admin/knowledge-health`) + Flight Recorder (`/admin/flight-recorder`)
  - Outbox drain cron (`/cron/knowledge-outbox`) + `npm run knowledge:e2e`
- Outcome Intelligence ([SPEC-013](docs/specs/SPEC-013_Outcome_Intelligence.md) / [ADR-008](docs/adr/ADR-008_Outcome_Intelligence.md))
  - `RecommendationOutcome` model + lifecycle (Generated → … → Successful / Unsuccessful / Inconclusive)
  - Strategy-level performance metrics (precision, recall, promotion, success, lead time)
  - Confidence calibration reports (empirical success by band — never mutates confidence)
  - Drift detection (strategy underperformance, false positives, acceptance, evidence quality)
  - Internal Intelligence Review dashboard (`GET /api/v1/outcome/review`, admin/manager)
  - `packages/max/outcome/` — OutcomeEngine; `POST /api/v1/outcome/records|lifecycle`
- Operator Intelligence ([SPEC-012](docs/specs/SPEC-012_Operator_Intelligence.md) / [ADR-007](docs/adr/ADR-007_Operator_Intelligence.md))
  - Interaction event model + RecommendationLearning aggregates
  - Explicit recommendation outcome lifecycle + internal trust/usefulness signal (never replaces confidence)
  - Adaptive Command Deck presentation (section order / visual dominance — never hide)
  - Max suggestion personalization from tenant conversational preferences
  - Internal Intelligence Quality Dashboard (`GET /api/v1/operator/quality`, admin/manager)
  - `packages/max/operator/` — OperatorEngine; `POST /api/v1/operator/events|outcomes`
- Live Intelligence Loop ([SPEC-011](docs/specs/SPEC-011_Live_Intelligence_Loop.md) / [ADR-006](docs/adr/ADR-006_Live_Intelligence_Evolution.md))
  - Common `IntelligenceEvent` model + lifecycle (Detected → Archived)
  - `packages/max/live/` — LiveLoopEngine, deck diff, material filter, awareness
  - Soft-poll evolution on Command Deck (gentle fade / one-shot movement; briefing accumulates)
  - Max awareness during active workspace sessions
  - Investigation continuity banner (“New intelligence available / Review”)
  - Per-entity live timeline; notifications limited to material events
  - `GET /api/v1/intelligence/live`, `/notifications`, `/timeline/:entityId`
- Intelligence Navigation ([SPEC-010](docs/specs/SPEC-010_Intelligence_Navigation.md))
  - Investigation trail + Related Intelligence on every node
  - Company Intelligence + Recommendation Detail composers (closes SPEC-006 remaining pages)
  - `GET /api/v1/recommendations/:id`, `GET /api/v1/companies/:id/intelligence`
  - Progressive evidence depth; Max evidence/related entities navigable; MaxContext synced to trail
  - Deep links `#/recommendation/:id`, `#/company/:id`, `#/evidence/:id` on `/command-deck`
- Command Deck UI ([SPEC-008](docs/specs/SPEC-008_Command_Deck_UI.md)) — render-only surface
  - `GET /command-deck` — Morning Brief, Highest Leverage Action, Intelligence Cards, Priority Queue, Ask Max launcher
  - Consumes only `CommandDeckModel` from `GET /api/v1/command-deck`
  - Staged reveal, composer empty states, calm error + last-successful recovery
  - Shell nav link for admin / manager / viewer / client; `/dashboard` unchanged
- Max Intelligence Workspace ([SPEC-009](docs/specs/SPEC-009_Max_Intelligence_Workspace.md))
  - Contextual full-height Ask Max modal from Command Deck entry points (Morning Brief, HLA, Priority Queue, Watch Alerts, launcher)
  - `packages/max/workspace/` — WorkspaceEngine, OpeningState, Suggestions, ResponseComposer, PresentationEngine
  - Deterministic Structured Response Object → Claude presentation only ([ADR-005](docs/adr/ADR-005_LLM_Presentation_Engine.md))
  - `POST /api/v1/max/workspace/open`, `POST /api/v1/max/workspace/ask`
  - Evidence panel + collapsed “Generated from” metadata; session context + switch acknowledgement
  - Legacy dashboard `/api/max/ask` unchanged

### Planned

- Wire Max agent (shadow) to `brief()` + `decide()` + `compose()` before side effects
- Durable IntelligenceEvent / Operator InteractionEvent / Outcome log / SSE transport
- Full Brevo/call/meeting dual-write coverage beyond touchpoint hooks
- One business week of Anchor operated entirely through Pulseforge (SPEC-014 success metric)

### Docs

- SPEC-018 Deterministic Replay & Temporal Reasoning Engine
- SPEC-013 Outcome Intelligence + ADR-008 Outcome Intelligence
- SPEC-012 Operator Intelligence + ADR-007 Operator Intelligence
- SPEC-011 Live Intelligence Loop + ADR-006 Live Intelligence Evolution
- SPEC-010 Intelligence Navigation
- SPEC-009 Max Intelligence Workspace + ADR-005 LLM Presentation Engine
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

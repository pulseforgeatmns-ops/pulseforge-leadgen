# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.9.2 |
| **Current Milestone** | Mission Engine product integration (SPEC-022 thin slice) |
| **Current Sprint** | SPEC-024 Discovery + SPEC-026 Ranking + SPEC-014 Knowledge Dual-Write |
| **Current Spec** | [SPEC-040 Mission Artifact Validation](docs/specs/SPEC-040_Mission_Artifact_Validation.md) — Implemented (v1 / ADR-026); [SPEC-039 Active Mission Resolver](docs/specs/SPEC-039_Active_Mission_Resolver.md) — Implemented (v1 / ADR-025); [SPEC-037 Operator Inbox](docs/specs/SPEC-037_Operator_Inbox.md) — Implemented (v1 / ADR-024); [SPEC-036 Outcome Intelligence](docs/specs/SPEC-036_Outcome_Intelligence.md) — Implemented (v1 / ADR-023); [SPEC-035 Direct Mail Execution](docs/specs/SPEC-035_Direct_Mail_Execution.md) — Implemented (v1); [SPEC-034 Campaign Review Workspace](docs/specs/SPEC-034_Campaign_Review_Workspace.md) — Implemented (v1); [SPEC-033 Mail Package Generator](docs/specs/SPEC-033_Mail_Package_Generator.md) — Implemented (v1); [SPEC-031 Business Signals](docs/specs/SPEC-031_Business_Signals_Capability.md) — Implemented (v1); [SPEC-028 Client Playbook](docs/specs/SPEC-028_Client_Playbook_Capability.md) — Implemented (v1); [SPEC-027B](docs/specs/SPEC-027B_Proposal_Generator_Capability.md) Implemented; [SPEC-026](docs/specs/SPEC-026_Opportunity_Ranking_Capability.md) Implemented; [SPEC-024](docs/specs/SPEC-024_Prospect_Discovery_Capability.md) Implemented |
| **Next Spec** | [SPEC-032 Mission Memory](docs/specs/SPEC-032_Mission_Memory.md) (Proposed / ADR-019) — durable messages/revisions on top of SPEC-039 attach path; [SPEC-030 Company Intelligence](docs/specs/SPEC-030_Company_Intelligence_Capability.md) (Proposed / ADR-017); then [SPEC-029 Execution Engine](docs/specs/SPEC-029_Execution_Engine.md) (multi-channel; Direct Mail channel via SPEC-035); Campaign Builder live adapter; [SPEC-015 Market Intelligence Domain](docs/specs/SPEC-015_Market_Intelligence_Domain.md) |
| **Current Priority** | Highest — Mission Memory (SPEC-032) on Active Mission Resolver + Artifact Validation; dual-write remains for live knowledge |
| **Last Completed** | SPEC-040 Mission Artifact Validation (v1 / ADR-026); SPEC-039 Active Mission Resolver (v1 / ADR-025); SPEC-037 Operator Inbox (v1 / ADR-024); SPEC-036 Outcome Intelligence (v1 / ADR-023); SPEC-035 Direct Mail Execution (v1 / ADR-022); SPEC-034 Campaign Review Workspace (v1 / ADR-021); SPEC-033 Mail Package Generator (v1); SPEC-031 Business Signals (ADR-018); SPEC-028 Client Playbook (ADR-015); SPEC-027B Proposal Generator; SPEC-026 Opportunity Ranking; SPEC-024 Prospect Discovery; SPEC-022/023 thin slice; SPEC-021–000 |
| **In Progress** | SPEC-014 dual-write operational readiness; SPEC-032 Mission Memory (proposed / ADR-019); SPEC-030 Company Intelligence (proposed; supersedes unfinished SPEC-025 enrichment); Campaign Builder live adapters |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; migration must be applied on Railway before dual-write fills the graph in prod; apply `migrations/2026-07-27-mission-engine.sql` (+ discovery profiles + proposal_versions + client_playbooks) for durable missions |
| **Upcoming Decisions** | When `/command-deck` becomes default landing vs `/dashboard`; one business week Anchor-only operation (SPEC-014 success metric) |

---

## Snapshot (2026-07-26)

### Intelligence layer

| Version | Capability |
|---|---|
| v0.7.1 | `KnowledgeService`, in-memory repo, evidence/claims, events, `explain()` |
| v0.7.2 | `GraphSyncEngine` — CRM/import/rebuild → KnowledgeService |
| v0.7.3 | `PersistentGraphRepository` (Postgres) — same interface; KnowledgeService unchanged |
| v0.7.4 | Query Engine — typed filters, traversal, timeline, path, metrics; enhanced `explain()` |
| v0.8.0 | Max Reasoning Engine — strategies, weighted score, independent confidence, explanations |
| v0.8.1 | Temporal Memory — snapshots, diffs, change detection, trends, watches (detection only) |
| v0.9.0 | Briefing Engine — assembles Knowledge + Reasoning + Memory into deterministic operator briefings |
| v0.9.1 | Policy & Decision Engine — allow / warn / requireApproval / block with immutable audit |
| v0.9.2 | Command Deck Composer — single immutable view model for the operator surface |
| v1.0.0 line | Workspace · Navigation · Live Loop · Operator Intelligence · Outcome Intelligence |
| SPEC-014 | Knowledge dual-write + outbox + Flight Recorder (admin) |
| SPEC-015A | Domain-neutral Reasoning Runtime + CRM Strategy Pack (no behavior change) |
| SPEC-017 | Domain Ontology Framework + Market Ontology (`@pulseforge/market-ontology`) |
| SPEC-018 | Deterministic Replay & Temporal Reasoning (`@pulseforge/replay`) |
| SPEC-019 | Evidence Laboratory (`@pulseforge/laboratory`) — isolated exploration |
| SPEC-020 | Evidence Query Language (`@pulseforge/eql`) — domain-neutral FIND/SHOW/REPLAY/COMPARE/EXPLAIN |
| SPEC-021 | Learning & Belief Evolution (`@pulseforge/learning`) — outcomes calibrate trust |

```bash
npm run test:knowledge
npm run test:dual-write
npm run test:market-ontology
npm run test:reasoning-runtime
npm run test:market-strategy
npm run test:replay
npm run test:laboratory
npm run test:eql
npm run test:learning
npm run test:capabilities
npm run test:mission
npm run knowledge:e2e
npm run test:knowledge:postgres
npm run test:max
```

### Operator surface

- `GET /api/v1/command-deck` → `CommandDeckModel` (SPEC-007) + `live` envelope from LiveLoop (SPEC-011) + **Operations** mission queue (SPEC-022)
- `GET /command-deck` → render-only UI (SPEC-008); Operations section + Mission Workspace; soft-poll evolution (SPEC-011); `/dashboard` remains available
- Shell **Operations** nav → `/command-deck#operations` (standalone agents tab is no longer the product Operations surface)
- `POST /api/v1/max/workspace/open|ask` → Max Intelligence Workspace (SPEC-009 / ADR-005) + awareness (SPEC-011); **business objectives route to Mission Engine first**
- `POST /api/max/ask` → legacy chat; same Mission IntentRouter gate
- `POST/GET /api/v1/missions` · `GET /api/v1/missions/:id` · `POST /api/v1/missions/:id/review` → Mission Engine API (SPEC-022)
- `GET /api/v1/recommendations/:id` → Recommendation Detail (SPEC-010)
- `GET /api/v1/companies/:id/intelligence` → Company Intelligence (SPEC-010)
- Investigation trail + Related Intelligence on `/command-deck` (SPEC-010); continuity banner (SPEC-011)
- `GET /api/v1/intelligence/live|notifications|timeline/:id` → Live Intelligence Loop (SPEC-011 / ADR-006)
- `POST /api/v1/operator/events|outcomes` · `GET /api/v1/operator/learning/:id|quality|preferences` → Operator Intelligence (SPEC-012 / ADR-007)
- `POST /api/v1/outcome/records|lifecycle` · `GET /api/v1/outcome/calibration|strategies|drift|review` → Outcome Intelligence (SPEC-013 / ADR-008)
- `GET /admin/knowledge-health` · `GET /admin/flight-recorder` → SPEC-014 admin operational confidence (admin/manager only)
- `POST/GET /cron/knowledge-outbox?secret=` → outbox drain worker

### Dual-write (SPEC-014)

- Producers: `dbClient.addCompany` / `addProspect` / `logTouchpoint`, Scout (`leadgen.js`), outcome records
- Path: outbox → `GraphSyncEngine.apply` → KnowledgeEventBus → KnowledgeStore
- Boot: `utils/knowledgeRuntime.js` + `utils/maxRuntime.js` share persistent Knowledge when `KNOWLEDGE_DUAL_WRITE` ≠ `0`
- Recovery: failed applies stay in `knowledge_outbox` and retry via cron / admin drain
- Flight Recorder stages: discovered → knowledge → reasoning → memory → briefing → deck → operator → outcome

### Still not live / deferred

- Full Brevo/call/meeting producer coverage beyond touchpoint + Scout hooks
- Durable cross-process Operator / Outcome / Live event logs (process-scoped today)
- Max agent consuming `compose()` before side effects (shadow-first)

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

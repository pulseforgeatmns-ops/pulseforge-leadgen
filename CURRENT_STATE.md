# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.9.2 |
| **Current Milestone** | Command Deck product surface (SPEC-006) |
| **Current Sprint** | SPEC-014 Knowledge Dual-Write & Operational Readiness |
| **Current Spec** | [SPEC-014 Knowledge Dual-Write](docs/specs/SPEC-014_Knowledge_Dual_Write.md) — In Progress |
| **Next Spec** | [SPEC-015 Market Intelligence Domain](docs/specs/SPEC-015_Market_Intelligence_Domain.md) (runtime unblocked by SPEC-015A); wire Max agent to `compose()` (shadow-first) |
| **Current Priority** | Highest — live knowledge dual-write so composers / workspace / live loop return live data |
| **Last Completed** | SPEC-021 Learning & Belief Evolution Engine; SPEC-020 Evidence Query Language (EQL); SPEC-019 Evidence Laboratory; SPEC-018 Deterministic Replay & Temporal Reasoning Engine; SPEC-017 Domain Ontology Framework & Market Ontology; SPEC-015A Reasoning Runtime Decoupling; SPEC-013 Outcome Intelligence; SPEC-012 Operator Intelligence; SPEC-011 Live Intelligence Loop; SPEC-010 Intelligence Navigation; SPEC-009 Max Workspace; SPEC-008 Command Deck UI; SPEC-007 Composer; SPEC-005 Policy; SPEC-004 Briefing; SPEC-003 Memory; SPEC-002 Reasoning; SPEC-001C Query; SPEC-001 Postgres; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | SPEC-014 dual-write, outbox retry, admin Validation Dashboard + Flight Recorder |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; migration must be applied on Railway before dual-write fills the graph in prod |
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
npm run knowledge:e2e
npm run test:knowledge:postgres
npm run test:max
```

### Operator surface

- `GET /api/v1/command-deck` → `CommandDeckModel` (SPEC-007) + `live` envelope from LiveLoop (SPEC-011)
- `GET /command-deck` → render-only UI (SPEC-008); soft-poll evolution (SPEC-011); `/dashboard` remains available
- `POST /api/v1/max/workspace/open|ask` → Max Intelligence Workspace (SPEC-009 / ADR-005) + awareness (SPEC-011)
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

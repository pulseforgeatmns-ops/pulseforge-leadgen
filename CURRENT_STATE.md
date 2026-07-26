# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.9.2 |
| **Current Milestone** | Command Deck product surface (SPEC-006) |
| **Current Sprint** | Outcome Intelligence shipped; shadow dual-write next |
| **Current Spec** | [SPEC-013 Outcome Intelligence](docs/specs/SPEC-013_Outcome_Intelligence.md) — Implemented |
| **Next Spec** | Shadow CRM/Scout → GraphSyncEngine dual-write; wire Max agent to `compose()` |
| **Current Priority** | Highest — live knowledge dual-write so composers / workspace / live loop return live market data |
| **Last Completed** | SPEC-013 Outcome Intelligence; SPEC-012 Operator Intelligence; SPEC-011 Live Intelligence Loop; SPEC-010 Intelligence Navigation; SPEC-009 Max Workspace; SPEC-008 Command Deck UI; SPEC-007 Composer; SPEC-005 Policy; SPEC-004 Briefing; SPEC-003 Memory; SPEC-002 Reasoning; SPEC-001C Query; SPEC-001 Postgres; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | Knowledge dual-write / production ingest |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; knowledge dual-write not live (composer / workspace / investigation / live loop fail closed until sync) |
| **Upcoming Decisions** | When `/command-deck` becomes default landing vs `/dashboard`; wire Max agent to `compose()` (shadow-first); durable event log when dual-write ships |

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

```bash
npm run test:knowledge
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

### Still not live

- Server/agent dual-write into the knowledge graph
- Default boot using persistent repository
- Max agent consuming ReasoningEngine / MemoryEngine / BriefingEngine / PolicyEngine / CommandDeckComposer with live knowledge
- Durable cross-process IntelligenceEvent / Operator InteractionEvent / Outcome log (process-scoped today)

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

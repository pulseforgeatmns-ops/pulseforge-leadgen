# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.9.2 |
| **Current Milestone** | Intelligence stack + composer complete — Command Deck UI is the v1.0 operator surface |
| **Current Sprint** | SPEC-006 Command Deck UI consuming `GET /api/v1/command-deck` / `max.compose()` |
| **Current Spec** | [SPEC-006 Pulseforge Command Deck](docs/specs/SPEC-006_Command_Deck.md) — Approved |
| **Next Spec** | Implement SPEC-006 UI; parallel: shadow dual-write (Scout/CRM → GraphSyncEngine) |
| **Current Priority** | Highest — intelligence-first Command Deck (consume stack via composer; do not recreate) |
| **Last Completed** | SPEC-007 CommandDeckComposer; SPEC-005 Policy; SPEC-004 Briefing; SPEC-003 Memory; SPEC-002 Reasoning; SPEC-001C Query; SPEC-001 Postgres; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | SPEC-006 UI (Morning Brief + HLA + Priority Queue + Ask Max) |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; knowledge dual-write not live (composer API returns empty-state-rich model until sync) |
| **Upcoming Decisions** | Command Deck feature-flag / route strategy; when to wire Max agent to `compose()` (shadow-first) |

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

```bash
npm run test:knowledge
npm run test:knowledge:postgres
npm run test:max
```

### Still not live

- Server/agent dual-write into the knowledge graph
- Default boot using persistent repository
- Max agent consuming ReasoningEngine / MemoryEngine / BriefingEngine / PolicyEngine / CommandDeckComposer with live knowledge

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

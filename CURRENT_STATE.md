# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.9.2 |
| **Current Milestone** | Command Deck UI (SPEC-008) — intelligence-first operator surface on `/command-deck` |
| **Current Sprint** | SPEC-008 render-only UI consuming `GET /api/v1/command-deck` |
| **Current Spec** | [SPEC-008 Command Deck UI](docs/specs/SPEC-008_Command_Deck_UI.md) — Implemented |
| **Next Spec** | SPEC-006 remaining: Ask Max workspace, Recommendation Detail, Company Intelligence; parallel shadow dual-write |
| **Current Priority** | Highest — calm briefing surface shipped; complete SPEC-006 investigation surfaces |
| **Last Completed** | SPEC-008 Command Deck UI; SPEC-007 CommandDeckComposer; SPEC-005 Policy; SPEC-004 Briefing; SPEC-003 Memory; SPEC-002 Reasoning; SPEC-001C Query; SPEC-001 Postgres; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | SPEC-006 remaining product surface (Ask Max workspace, explainability pages) |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; knowledge dual-write not live (composer API returns empty-state-rich model until sync) |
| **Upcoming Decisions** | When `/command-deck` becomes default landing vs `/dashboard`; wire Max agent to `compose()` (shadow-first) |

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

### Operator surface

- `GET /api/v1/command-deck` → `CommandDeckModel` (SPEC-007)
- `GET /command-deck` → render-only UI (SPEC-008); `/dashboard` remains available

### Still not live

- Server/agent dual-write into the knowledge graph
- Default boot using persistent repository
- Max agent consuming ReasoningEngine / MemoryEngine / BriefingEngine / PolicyEngine / CommandDeckComposer with live knowledge
- Ask Max conversation workspace (SPEC-006)

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

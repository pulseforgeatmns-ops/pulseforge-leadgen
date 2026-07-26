# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.8.1 |
| **Current Milestone** | Temporal Intelligence & Memory |
| **Current Sprint** | SPEC-003 complete; next shadow CRM→sync and/or briefing/operator surfaces |
| **Current Spec** | [SPEC-003 Temporal Intelligence & Memory](docs/specs/SPEC-003_Temporal_Intelligence_Memory.md) — Done |
| **Next Spec** | Shadow dual-write (Scout/CRM → GraphSyncEngine) and/or v0.9.0 operator/conversation surfaces |
| **Current Priority** | High — controlled shadow emit into persistent graph; optional Max→Reasoning/Memory wiring (shadow) |
| **Last Completed** | SPEC-003 Memory; SPEC-002 Reasoning Engine; SPEC-001C Query Engine; SPEC-001 Postgres store; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | None |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; knowledge/reasoning/memory not wired into server/agents |
| **Upcoming Decisions** | When to enable shadow dual-write; when to wire Max agent to ReasoningEngine + MemoryEngine (shadow-first) |

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

```bash
npm run test:knowledge
npm run test:knowledge:postgres
npm run test:max
```

### Still not live

- Server/agent dual-write into the knowledge graph
- Default boot using persistent repository
- Max agent consuming ReasoningEngine / MemoryEngine

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.7.4 |
| **Current Milestone** | Knowledge query engine |
| **Current Sprint** | SPEC-001C complete; next shadow CRM→sync wiring |
| **Current Spec** | [SPEC-001C Knowledge Query Engine](docs/specs/SPEC-001C_Knowledge_Query_Engine.md) — Done |
| **Next Spec** | Shadow dual-write (Scout/CRM → GraphSyncEngine) and/or broader Business KG production ingest |
| **Current Priority** | High — controlled shadow emit into persistent graph |
| **Last Completed** | SPEC-001C Query Engine; SPEC-001 Postgres store; SPEC-001B sync; SPEC-001A foundation; SPEC-000 docs |
| **In Progress** | None |
| **Known Blockers** | Inquiry Foundation production deploy blocked; Max orchestration shadow-default; knowledge not wired into server/agents |
| **Upcoming Decisions** | When to enable shadow dual-write; whether default runtime should prefer Postgres when `DATABASE_URL` is present |

---

## Snapshot (2026-07-26)

### Knowledge layer

| Version | Capability |
|---|---|
| v0.7.1 | `KnowledgeService`, in-memory repo, evidence/claims, events, `explain()` |
| v0.7.2 | `GraphSyncEngine` — CRM/import/rebuild → KnowledgeService |
| v0.7.3 | `PersistentGraphRepository` (Postgres) — same interface; KnowledgeService unchanged |
| v0.7.4 | Query Engine — typed filters, traversal, timeline, path, metrics; enhanced `explain()` |

```bash
npm run test:knowledge
npm run test:knowledge:postgres
```

### Still not live

- Server/agent dual-write into the knowledge graph
- Default boot using persistent repository

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

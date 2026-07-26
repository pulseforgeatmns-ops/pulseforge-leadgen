# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.7.1 |
| **Current Milestone** | Knowledge Layer Foundation |
| **Current Sprint** | SPEC-001A complete; next persistent repository / SPEC-001 |
| **Current Spec** | [SPEC-001A Knowledge Layer Foundation](docs/specs/SPEC-001A_Knowledge_Layer_Foundation.md) — Done |
| **Next Spec** | [SPEC-001 Business Knowledge Graph](docs/specs/SPEC-001_Business_Knowledge_Graph.md) (persistent store + shadow ingest) |
| **Current Priority** | High — choose and implement durable `GraphRepository` without changing KnowledgeService API |
| **Last Completed** | SPEC-001A `packages/knowledge/` (in-memory KnowledgeService, evidence/claims, events, explain); SPEC-000 docs foundation (v0.7.0) |
| **In Progress** | None on knowledge persistence yet |
| **Known Blockers** | Inquiry Foundation production deploy blocked pending real tenant + approved sender; Max orchestration remains shadow-default; no persistent KG repository yet; Scout/CRM not wired to knowledge events |
| **Upcoming Decisions** | Persistent GraphRepository backend (Postgres vs other) under SPEC-001 / ADR-004; shadow ingest wiring for Scout |

---

## Snapshot (2026-07-26)

### What works in production today

- Multi-client Postgres CRM (`clients`, `companies`, `prospects`, touchpoints, agent_log)
- Scout lead scraping + ICP scoring (including Anchor `cleaning_buyer` profile)
- Setter / closer dashboards and handoff flows
- Emmett email sequences (Brevo), Riley inbound triage, social agents with human approval
- Max daily briefing + Max prospect orchestration **shadow** path
- Scorecard → Brevo sync paths; Anchor verified queue tooling

### What is intentionally not live

- `packages/knowledge` — library only; not wired into `server.js` / agents
- Inquiry Foundation / Operator Command Center / outbound outbox — local & shadow-only
- Max non-shadow state transitions and automated outreach actions — flags default off
- Persistent Business Knowledge Graph — not started (SPEC-001)

### Knowledge layer (v0.7.1)

| Piece | Location |
|---|---|
| Public API | `KnowledgeService` via `packages/knowledge` |
| Storage (001A) | `InMemoryGraphRepository` only |
| Ingest | `KnowledgeEventBus` + `KnowledgeIngestor` |
| Explain | `knowledge.explain(tenantId, nodeId)` |

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

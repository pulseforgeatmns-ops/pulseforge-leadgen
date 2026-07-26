# CURRENT_STATE

> Project heartbeat. Update on every PR that changes version, priority, blockers, or active work.

| Field | Value |
|---|---|
| **Version** | v0.7.0 |
| **Current Milestone** | Repository Foundation & Source of Truth |
| **Current Sprint** | Close SPEC-000; begin SPEC-001 planning spike |
| **Current Spec** | [SPEC-000 Repository Foundation](docs/specs/SPEC-000_Repository_Foundation.md) — Done |
| **Next Spec** | [SPEC-001 Business Knowledge Graph](docs/specs/SPEC-001_Business_Knowledge_Graph.md) |
| **Current Priority** | Critical — start Business Knowledge Graph (v0.8.0) |
| **Last Completed** | SPEC-000 Repository Foundation (v0.7.0 docs hierarchy); prior: lead-gen CRM, Max shadow orchestration Phases 1–2.5, Inquiry Foundation (shadow/local), Anchor verified queue / phone setter, revenue Phase 16B tooling |
| **In Progress** | Ready for SPEC-001 (no KG implementation started) |
| **Known Blockers** | Inquiry Foundation production deploy blocked pending real tenant + approved sender; Max orchestration remains shadow-default; Knowledge Graph not yet implemented |
| **Upcoming Decisions** | KG storage shape (SPEC-001 / ADR-004); Max reasoning authority boundaries (SPEC-002); conversation UI sequencing for v1.0 |

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

- Inquiry Foundation / Operator Command Center / outbound outbox — local & shadow-only; production deploy not authorized
- Max non-shadow state transitions and automated outreach actions — flags default off
- Business Knowledge Graph (SPEC-001) — not started
- Max Reasoning Engine as product surface (SPEC-002) — not started

### Active clients (reference)

| ID | Client | Notes |
|---|---|---|
| 1 | Pulseforge (NH) | Primary lead-gen pipeline |
| 2 | MSHI (WV) | Renovation customer/referral Scout plan |
| 5 | Pulseforge Nashville | Multi-vertical Scout |
| 10 | Anchor Cleaning | Separate LLC; `scoring_profile=cleaning_buyer`; Scout-focused |

---

## How to update this file

When you finish a slice:

1. Move completed work to **Last Completed**.
2. Set **In Progress** / **Current Spec** / **Next Spec**.
3. Refresh **Known Blockers** and **Upcoming Decisions**.
4. Bump **Version** only when a release doc says so.

# SPEC-104 — Persistent Operator Context

| Field | Value |
|---|---|
| **Status** | Implemented (v1.0.0) |
| **Target Version** | v1.0.0 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Created** | 2026-08-17 |

## Objective

Max begins every conversation already understanding who the client is, what they are trying to accomplish, what changed recently, what remains unresolved, and what should happen next. The operator should never feel like they are starting over.

## Problem

Blueprint, Playbook, Mission Engine, Outcome Intelligence, Paige learning, Knowledge Graph, Memories, and Evidence are powerful but independent. Max lacks a persistent operational identity per business.

## Solution

Every client gets one living **Operator Context** document — Max's working memory (not permanent facts, not historical records). It is rebuilt when meaningful events occur; session briefs are generated at open time and never stored.

### Context sections

| Section | Source |
|---|---|
| Identity | Approved Blueprint |
| Objectives | `operator_objectives` |
| Current Priorities | Active objectives → Blueprint goals |
| Active Missions | Mission Engine (reference only, no duplication) |
| Known Risks | Blueprint unknowns, playbook constraints |
| Opportunities | Playbook channels, differentiation |
| Recent Outcomes | Content outcomes, completed missions |
| Open Questions | Blueprint unknowns, thin pipeline signals |
| Recommendations | **Generated at brief time — not stored** |

### Update triggers

Rebuild on: interview completed, blueprint approved, campaign launched, walkthrough booked, job won, content published, mission completed, client message, outcome recorded, playbook updated, operator objective changed.

### Startup sequence

```text
Load Blueprint → Load Playbook → Load Operator Context →
Load recent Outcomes → Load active Missions → Generate Brief → Conversation begins
```

## Data Model

Table: `operator_contexts`

| Column | Notes |
|---|---|
| `tenant_id` | TEXT NOT NULL |
| `client_id` | INTEGER NOT NULL — one row per client |
| `version` | INTEGER — incremented on each rebuild |
| `context` | JSONB — living document |
| `last_rebuild_trigger` | TEXT |
| `last_rebuild_at` | TIMESTAMPTZ |

Table: `operator_context_rebuild_events` — audit trail for versioning.

Migration: `migrations/2026-08-17-operator-context.sql`

## Implementation

| Component | Path |
|---|---|
| Service | `services/operatorContext.js` |
| Event hooks | `services/operatorContextEvents.js` |
| Startup loader | `packages/max/workspace/OperatorContextLoader.js` |
| Workspace integration | `WorkspaceEngine.open()`, `OpeningStateBuilder` |
| API | `GET /api/v1/operator-context`, `POST /api/v1/operator-context/rebuild` |
| UI | Command Deck workspace — "Reviewed before you arrived" badge |

## Dependencies

- SPEC-083/085 Client Intelligence / Blueprint
- SPEC-028 Client Playbook
- SPEC-095 Operator Objectives
- SPEC-022 Mission Engine
- SPEC-092 Content Outcome Intelligence
- SPEC-103 Durable Business Understanding Retrieval

## Acceptance

- [x] Persistent `operator_contexts` model with versioning
- [x] Context builder aggregates Blueprint + Playbook + Objectives + Missions + Outcomes
- [x] Event-driven rebuild pipeline (blueprint approve, objective change, extensible hooks)
- [x] Startup loading pipeline on workspace open
- [x] Brief generator from current context (not stored)
- [x] Unit and integration tests
- [x] UI support for "Reviewed before you arrived" brief

## Out of Scope (v1)

- LLM-generated brief prose (deterministic assembly only)
- Knowledge Graph query wiring (SPEC-103 future work)
- Automatic extraction from client messages
- Cross-client operator context rollup

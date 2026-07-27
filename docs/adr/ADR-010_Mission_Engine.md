# ADR-010 — Mission Engine

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) |
| **Supersedes** | — |

## Context

Operators today invoke named agents (“Run Scout”). That leaks platform architecture and forces customers to learn internal modules. Max already reasons and briefs (SPEC-002–013) but cannot durable-plan or execute multi-step business objectives. Outreach and brand-visible actions remain gated by [ADR-003](ADR-003_Human_Approval.md).

We need a control plane where operators express **intent** and the system selects **capabilities**, without Max hardcoding agent names, and without auto-sending.

## Decision

1. **Mission Engine** is Max’s orchestration layer for business objectives ([SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md)).
2. **Capability Registry** is the only way missions invoke work. Product APIs and operator-facing copy use capability names (Prospect Discovery, Knowledge Update, Reasoning, Campaign Builder), not agent module names.
3. **Missions are durable** — full lifecycle (Requested → … → Archived) survives process restart; steps are retryable without losing completed progress.
4. **Review Mode is mandatory** before any customer-visible outreach. Completing a mission produces reviewable deliverables; Approve / Edit / Reject / Run Again are explicit. This reaffirms ADR-003.
5. **Legacy agent endpoints remain** (`/api/run/:agent`, cron). Mission Engine is an additional control plane, not a forced rewrite of every agent in v1.
6. Numbering: product draft labeled this “SPEC-015”; repository **SPEC-015** remains Market Intelligence Domain. Implementation contract is **SPEC-022**.

## Consequences

### Positive

- Intent-first UX; agents become implementation details
- Plug-in capabilities for new domains without redesigning Max
- Audit / replay aligned with Evidence Platform culture (ADR-009)
- Clear human gate before send

### Negative / tradeoffs

- Parallel control planes until dashboards deprecate “Run Scout” language
- Planner must stay deterministic for v1 types (open-ended NL planning deferred)
- Requires durable schema + progress UX work

### Follow-ups

- Implement SPEC-022 package + migrations behind `MISSION_ENGINE` flag
- Wire Campaign Creation path for Anchor as first success metric
- Update Command Deck / Workspace copy away from agent-run verbs over time
- Link from DECISIONS.md, specs index, CURRENT_STATE when implementation starts

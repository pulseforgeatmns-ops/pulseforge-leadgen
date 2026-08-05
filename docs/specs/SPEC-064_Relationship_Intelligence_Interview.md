# SPEC-064 — Relationship Intelligence Interview v1

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-04 |
| **Depends** | [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) (evidence before reasoning) |

## Objective

After every meaningful sales interaction, Max conducts a natural debrief and converts what happened into structured, reviewable relationship intelligence. Success: a user can provide notes (or answer one question at a time), review a draft, commit durable interaction + insight rows, and query them — without CRM/opportunity mutation and without feeling like a CRM form.

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [SPEC-066](SPEC-066_Max_Market_Intelligence_Integration.md) — Max as consumer of evidence domains (parallel pattern)
- [SPEC-070](SPEC-070_Intelligence_Seed_Libraries.md) — Relationship Intelligence facts stay separate from seed libraries

## Problem

Market Intelligence v1 is complete enough for read-only use. The system still lacks Relationship Intelligence: the permanent memory of calls, walkthroughs, estimates, meetings, demos, proposals, and follow-ups. Without a guided debrief, truth decays into free-text notes or is never captured.

## Scope

1. Durable tables: `relationship_interactions`, `relationship_interaction_insights`
2. Max-owned interview service (state-machine, notes mode first): start / answer / summarize / commit
3. Structured draft payload with `isEvidence: true` and review-before-commit
4. CLI notes mode: `npm run relationship:intel:interview`
5. Internal admin GET/POST APIs under `/api/v1/relationship-intel/*`
6. Tests for interview lifecycle, guards, validation, and no CRM writes

## Out of Scope

- Cal coaching
- Forecast changes / opportunity stage mutation
- Outcome learning
- Automatic follow-up generation
- Composer outreach / autonomous actions
- Call recording / transcription ingestion
- Third-party meeting integrations
- Interactive CLI prompt mode (deferred; notes mode ships first)
- Max WorkspaceEngine domain routing
- Knowledge dual-write of relationship facts

## Dependencies

- Postgres + `pgcrypto` for UUIDs
- Session auth (`requireAuth` + `requireRole('admin', 'manager')`)
- No dependency on Mission Engine or Market Intelligence tables

## Architecture

```
CLI notes | Admin API
        ↓
services/relationshipIntelligenceInterview.js   (Max owns capture)
        ↓
relationship_interactions (draft → reviewed → committed)
relationship_interaction_insights
        ↓ (future)
Cal coaching (read-only consumer — not in v1)
```

Max captures and structures interaction intelligence. Cal later reviews and coaches. CRM/opportunity updates remain out of scope.

Interview id equals draft interaction id. Turn state lives in `interview_state JSONB`. Soft TEXT entity refs — no FK to CRM.

## Data Model

### `relationship_interactions`

| Column | Notes |
|---|---|
| `id` | UUID PK |
| `client_id` | INTEGER nullable (tenant scope) |
| `company_id`, `contact_id`, `opportunity_id`, `user_id` | TEXT nullable, soft refs |
| `interaction_type` | cold_call, discovery_call, walkthrough, estimate, meeting, demo, proposal_review, follow_up, other |
| `occurred_at` | timestamptz |
| `source` | e.g. `cli_notes`, `api`, `max_interview` |
| `status` | draft \| reviewed \| committed |
| `raw_summary`, `structured_summary`, `confidence` | filled on summarize |
| `interview_state` | JSONB turn machine |
| `created_at`, `updated_at` | |

### `relationship_interaction_insights`

| Column | Notes |
|---|---|
| `id` | UUID PK |
| `interaction_id` | FK CASCADE |
| `kind` | pain, goal, objection, timeline, budget, decision_maker, stakeholder, competitor, next_step, commitment, risk, buying_signal, open_question, preference, context |
| `label`, `value`, `confidence`, `source_quote` | |
| `created_at` | |

## Implementation Plan

1. Spec + README registry
2. Additive migration + rollback
3. Interview service (state machine + notes heuristics)
4. Admin routes + server mount
5. CLI + npm script
6. Readiness/acceptance report (`relationship:intel:readiness`, GET readiness, `--accept` fixture)
7. Unit + route smoke tests
8. CURRENT_STATE / CHANGELOG

## Migration Strategy

- Forward: additive `CREATE TABLE IF NOT EXISTS`
- Rollback: drop insights then interactions
- Compatibility: no CRM schema changes

## Testing

- Unit: start, answer, summarize, refuse commit pre-summary, commit, notes draft, low-info caveats, insight kind validation, CRM SQL allowlist
- Readiness: blocked/partial/ready derivation, acceptance fixture (notes→summarize→commit), no CRM writes, CLI/route wiring
- Route: auth gates, payload validation, endpoint registration

## Acceptance Criteria

- [x] User can provide notes from a sales interaction
- [x] Max produces structured relationship intelligence
- [x] User can review before commit
- [x] Committed interactions are queryable
- [x] Insight records are durable
- [x] Tests pass
- [x] No unrelated CRM/opportunity writes
- [x] Flow feels like a debrief (one question at a time / notes), not a CRM form
- [x] Readiness report covers tables, enums, counts, commit exercise, CRM mutation detection
- [x] `--accept` creates a safe committed fixture and verifies queryability

## Future Work

- Interactive CLI prompt mode
- Max Workspace domain routing for conversational debrief
- Cal coaching consumer of committed interactions
- Knowledge dual-write of relationship facts
- Edit UI for draft insights before commit

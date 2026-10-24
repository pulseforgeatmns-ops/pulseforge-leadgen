# SPEC-075 — Service Mode Operator Loop v1

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Max Platform |
| **Created** | 2026-08-04 |
| **Depends** | Prospect Operating Brief (SPEC-074), [SPEC-064](SPEC-064_Relationship_Intelligence_Interview.md), [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

Make Max useful every day without granting autonomous execution. The system inspects active intelligence, identifies what deserves Jake’s attention, and produces a read-only manual action queue answering: what should Jake work on next?

## Vision References

- [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) — Evidence Before Reasoning
- [ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md) — Intelligence Before Execution
- [SPEC-064](SPEC-064_Relationship_Intelligence_Interview.md) — Relationship Intelligence Interview
- Prospect Operating Brief (SPEC-074) — per-target synthesis consumed by this loop

## Problem

Market Intelligence, Relationship Intelligence, and Prospect Operating Briefs are useful when Jake asks for them, but the system does not proactively surface what needs attention. AS Cleaning Co. exposed a real service-mode need: capture a live opportunity, generate a brief, recommend a manual next action — without autonomous execution.

## Scope

1. Service: `services/serviceModeOperatorLoop.js` → `getServiceModeOperatorLoop(options)`
2. CLI: `npm run operator:service-loop`
3. GET-only admin API: `GET /api/v1/operator/service-loop`
4. Max read-only adapter: `packages/max/workspace/ServiceModeOperatorLoopContext.js`
5. Translate Prospect Operating Brief suggestions into operator queue items
6. Scan recent committed Relationship Intelligence when no target id is provided

## Out of Scope

- Autonomous sends
- Composer generation
- CRM mutation
- Task creation
- Calendar scheduling
- Service agreement generation
- Dashboard UI
- Outcome tracking
- Cal coaching

## Dependencies

- SPEC-064 committed `relationship_interactions` / insights
- SPEC-074 `getProspectOperatingBrief`
- Session auth (`requireAuth` + `requireRole('admin', 'manager')`)

## Architecture

```
CLI | GET /api/v1/operator/service-loop | Max inspection
        ↓
services/serviceModeOperatorLoop.js
        ↓ list committed RI candidates (filter / dedupe)
        ↓ getProspectOperatingBrief per candidate
        ↓ map brief → manual action queue
        ↓
{ kind: service_mode_operator_loop, isEvidence: false, actions[], caveats[] }
```

Every action stamps `autonomousExecution: false`. No outbound, no CRM writes.

## Allowed Action Types

- `prepare_service_agreement`
- `send_follow_up`
- `schedule_kickoff`
- `prepare_proposal`
- `clarify_open_questions`
- `research_company`
- `link_crm_record`
- `manual_review`
- `wait_for_reply`

## Candidate Selection

When no specific id is provided, scan recent committed relationship interactions (default window 14 days). Prioritize buying signals, next steps, commitments, open questions, proposal/pilot/service-agreement language, and unmatched CRM soft-refs. Ignore readiness fixtures, placeholder notes (“Paste notes here”), thin evidence, and duplicate raw summaries.

## Priority Rules

- **high** — buying signal + seller-side next step; service agreement / kickoff / pilot language; final questions before moving forward
- **medium** — weaker buying signal with open questions; follow-up after discovery; useful RI without CRM identity
- **low** — no seller-side action; waiting on prospect; thin evidence

## Implementation Plan

1. Spec + README registry
2. Operator loop service + brief translation
3. CLI + GET route + Max adapter
4. Tests (empty queue, committed-only, placeholders, dedupe, AS Cleaning, no writes)

## Testing

- `test/serviceModeOperatorLoop.test.js`
- `test/serviceModeOperatorLoopCli.test.js`
- `test/serviceModeOperatorLoopRoutes.test.js`
- `packages/max/workspace/tests/serviceModeOperatorLoopContext.test.js`

## Acceptance Criteria

- [x] Jake can run one command and see a prioritized manual action queue
- [x] AS Cleaning Co. surfaces as high priority with prepare_service_agreement or schedule_kickoff
- [x] Queue items include rationale and source references
- [x] No autonomous execution
- [x] Tests cover empty queue, committed-only scan, placeholders, dedupe, brief usage, CRM caveat, CLI, API, Max adapter

## Future Work

- Dashboard UI for the operator queue
- Outcome tracking after manual completion
- Calendar / Composer / CRM write-backs (still gated by intelligence-before-execution)

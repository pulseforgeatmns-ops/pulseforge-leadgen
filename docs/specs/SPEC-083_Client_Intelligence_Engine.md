# SPEC-083 — Client Intelligence Engine (CIE)

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-06 |
| **Depends on** | [PRD-001 Client Intelligence Interview](../vision/) (product intent); [SPEC-028 Client Playbook](SPEC-028_Client_Playbook_Capability.md); [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md); [ADR-045](../adr/ADR-045_Evidence_Before_Reasoning.md) |
| **Consumed by** | Client Playbook (SPEC-028) — draft generation only; Scout / Composer / Campaigns consume playbooks, not blueprints |

## Objective

Build and maintain Pulseforge’s understanding of a client’s business via an adaptive (v1: fixed-bank) text interview that produces an evidence-backed **Business Blueprint**.

The CIE is the authoritative source of **client understanding**. It does **not** own outbound strategy.

**Architectural invariant (verbatim):**

> The Client Intelligence Engine does not generate campaign strategy directly. Its responsibility ends at producing an approved Business Blueprint. Upon approval, the Blueprint is committed into the versioned Client Playbook (SPEC-028), which remains the operational strategy artifact consumed by Scout, Composer, and downstream systems.

**“Committed into” means (v1):**

1. Generate a `pending_review` Client Playbook from the approved Blueprint (understanding → mapped fields only).
2. An **operator** still reviews that playbook.
3. An **operator** activates the playbook (SPEC-028 `approveVersion` / activation).
4. **No** automatic campaign, Scout, or Composer activation.

Success for v1: onboard Aji through a text interview → evidence-backed Blueprint → client edit/approve → `pending_review` playbook exists for the existing operational workflow.

## Vision References

- [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)
- [SPEC-028 Client Playbook Capability](SPEC-028_Client_Playbook_Capability.md)
- [SPEC-064 Relationship Intelligence Interview](SPEC-064_Relationship_Intelligence_Interview.md) (interview pattern reference)

## Problem

Static onboarding forms do not produce durable, evidence-backed understanding. Client Playbooks (SPEC-028) need a richer upstream source of truth for *who the business is*, without CIE inventing *how Pulseforge should sell*.

## Scope (v1 thin slice)

- Interview session with fixed `QUESTION_BANK` (text only)
- Chat UI at `/client-intel`
- Evidence extraction after every client turn (`EXPLICIT` / `INFERRED` / `OBSERVED` / `CLIENT_EDITED`)
- Simple confidence (no length scoring; explicit / consistency / confirmation / corroboration only)
- Business Blueprint generation with sections + unknowns
- Client review / edit / approve
- On approve: immutable blueprint snapshot + `pending_review` Client Playbook handoff
- Persist sessions, turns, evidence, blueprint versions
- APIs listed below
- Traceability: every generated playbook section maps to ≥1 blueprint section

## Out of Scope

- Voice / realtime STT
- Adaptive LLM interviewing
- Advanced confidence models
- Recalibration session UI
- Max Workspace integration
- Campaign generation, prospect scoring, sequences
- Automatic Scout / Composer / campaign activation
- Inventing `preferredChannels`, `offers`, or `outreachSequence` in the handoff

## Separation of concerns

```text
Interview
    │
    ▼
Business Blueprint   ← understanding (who is this business?)
    │
    ▼
Client Playbook      ← strategy (how should Pulseforge grow them?)
    │
    ▼
Scout / Composer / Campaigns / Reports
```

## Architecture

```text
Client
  → Interview Session
  → Conversation Engine (fixed QUESTION_BANK)
  → Evidence Extractor
  → Confidence Engine (simple)
  → Business Blueprint Generator
  → Client Approval
  → Approved Business Blueprint
  → pending_review Client Playbook (SPEC-028)
  → Operator review / activate
```

## Interview lifecycle

Allowed transitions only (no skipping):

```text
NEW → DISCOVERY → CLARIFICATION → VALIDATION → BLUEPRINT_GENERATION → CLIENT_REVIEW → APPROVED
```

## Data model

### `cie_interview_sessions`

`id`, `client_id`, `status`, `started_at`, `completed_at`, `current_stage`, `summary`, `confidence_score`, `interview_state` JSONB

### `cie_interview_turns`

`id`, `session_id`, `speaker`, `message`, `timestamp`, `goal`, `asked_because`, `derived_evidence` JSONB

### `cie_evidence`

`id`, `client_id`, `session_id`, `source`, `source_turn_id`, `category`, `statement`, `confidence`, `type` (`EXPLICIT`|`INFERRED`|`OBSERVED`|`CLIENT_EDITED`), `created_at`

### `cie_business_blueprints`

`id`, `client_id`, `session_id`, `version`, `status` (`draft`|`in_review`|`approved`|`superseded`), `generated_by`, sections JSONB, `confidence_summary` JSONB, `playbook_id`, `playbook_version`, revision metadata

**Sections:** identity, services, idealCustomers, avoidCustomers, targetMarkets, competitiveAdvantages, brandVoice, campaignGoals, successMetrics — each `{ summary, confidence, evidenceIds[], unknowns[] }`.

## Public API

| Method | Path |
|---|---|
| POST | `/api/v1/clients/:id/interview/start` |
| POST | `/api/v1/interview/:id/message` |
| GET | `/api/v1/interview/:id` |
| GET | `/api/v1/interview/:id/blueprint` |
| POST | `/api/v1/blueprint/:id/revise` |
| POST | `/api/v1/blueprint/:id/approve` |
| GET | `/api/v1/clients/:id/blueprint` |

## Playbook handoff invariant

> The Playbook may only contain strategy directly supported by the approved Business Blueprint or explicitly added by an operator.

CIE maps understanding fields only; strategy fields remain empty for operator fill.

## Implementation Plan

1. Spec + registry + CURRENT_STATE
2. Migration `cie_*` tables
3. Interview service (memory + Postgres stores)
4. Blueprint revise / approve + playbook handoff
5. Routes + `/client-intel` UI + CLI
6. Tests + CHANGELOG

## Migration Strategy

Additive SQL migration with rollback script. No backfill required. Compatible with existing `client_playbooks`.

## Testing

- Lifecycle transitions (no skip)
- Evidence after every turn
- Confidence rules (no length scoring)
- Blueprint generation + immutable approved versions
- Playbook handoff (`pending_review`, empty strategy fields, section provenance)
- Route auth registration

## Acceptance Criteria

- [x] Client can complete a text interview
- [x] Evidence extracted after every client response
- [x] Confidence calculated per blueprint section (simple rules)
- [x] Business Blueprint generated with unknowns
- [x] Client can edit and approve the blueprint
- [x] Approved blueprint never overwritten (new version on revise)
- [x] Approval creates `pending_review` Client Playbook
- [x] Every generated Client Playbook section is traceable to ≥1 Business Blueprint section
- [x] No Scout/Composer/campaign activation from CIE

## Future Work

- Voice interview
- LLM-adaptive questioning
- Richer confidence / contradiction resolution
- Recalibration sessions
- Max Workspace surface
- Operator playbook editor UI for strategy fields

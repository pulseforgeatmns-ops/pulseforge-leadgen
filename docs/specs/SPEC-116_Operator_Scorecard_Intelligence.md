# SPEC-116 — Operator Scorecard Intelligence (OSI)

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-19 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md), [SPEC-085](SPEC-085_Executive_Business_Brief.md), [SPEC-013](SPEC-013_Outcome_Intelligence.md), [SPEC-095](SPEC-095_Max_Durable_Operator_Objectives.md), [SPEC-112](SPEC-112_Acquisition_Intelligence_Model.md) |
| **ADR** | [ADR-053 Business Success Is Operator-Defined](../adr/ADR-053_Business_Success_Is_Operator_Defined.md) |

## Objective

Transform operator goals into an approved business scorecard that becomes PulseForge's definition of success for that business.

Traditional CRMs ask operators what metrics they want to track. PulseForge understands the business first, reasons about its objectives, and recommends the metrics most likely to predict success.

Operators remain the final decision-makers.

Max recommends. Operators approve. The approved scorecard becomes the operational definition of business success until the operator chooses to change it.

## Vision References

- [ADR-053 Business Success Is Operator-Defined](../adr/ADR-053_Business_Success_Is_Operator_Defined.md)
- [ADR-003 Human Approval](../adr/ADR-003_Human_Approval.md)
- [ADR-007 Operator Intelligence](../adr/ADR-007_Operator_Intelligence.md)
- [ADR-008 Outcome Intelligence](../adr/ADR-008_Outcome_Intelligence.md)
- [ADR-021 Human Approval Before Execution](../adr/ADR-021_Human_Approval_Before_Execution.md)
- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md)
- [SPEC-013 Outcome Intelligence](SPEC-013_Outcome_Intelligence.md)
- [SPEC-095 Durable Operator Objectives](SPEC-095_Max_Durable_Operator_Objectives.md)
- [SPEC-112 Acquisition Intelligence Model](SPEC-112_Acquisition_Intelligence_Model.md)

## Philosophy

```text
Traditional CRM
Ask → Store → Report

PulseForge
Understand → Reason → Generate Draft Scorecard → Operator Review
  → Approved Scorecard → Measure → Learn → Evolve
```

## Problem

Most operators track metrics because they have always tracked them — not because they are the best indicators of success.

Two businesses with identical revenue may require completely different scorecards depending on:

- business model
- growth stage
- acquisition strategy
- operational constraints
- stated objectives

PulseForge should help operators identify the scorecard most appropriate for their business rather than expecting them to design one themselves.

## Guiding Principles

### Business Outcomes Drive Metrics

Metrics exist to measure progress toward business outcomes. Max reasons from:

- Business Blueprint
- Acquisition Intelligence Model
- Business stage
- Revenue model
- Operator objectives

before recommending metrics.

### Max Advises

Max recommends. The operator decides. Recommendations are never automatically adopted.

### Operator Authority

The business scorecard belongs to the operator. Only the operator may accept, modify, remove, reorder, or add metrics. The approved scorecard becomes the authoritative definition of business success for that tenant.

### Recommendations Must Be Explainable

Every metric recommendation must include:

- why it matters
- what business outcome it supports
- why Max believes it belongs on the scorecard

## Scope (v1 thin slice)

1. Deterministic reasoning pipeline: business understanding → objectives → stage → model → outcome intelligence → draft scorecard
2. Explainable metric recommendations (name, reason, outcome supported, confidence, leading/lagging, category)
3. Operator review: accept / modify / add / remove / reorder
4. Optional removal-reason prompt; feedback becomes Operator Intelligence
5. Operator approval promotes a tenant-scoped approved scorecard
6. Draft scorecards are never used for reporting
7. Executive Business Brief replaces **Success Looks Like** with Recommended / Approved / Under Review sections
8. Daily Briefings use the approved scorecard as the definition of business success
9. Periodic evolution recommendations as the business stage changes — nothing auto-applies
10. In-memory store + Postgres persistence + APIs + review UI
11. Competency `operator_scorecard_intelligence`

## Out of Scope

- Automatically adopting Max recommendations
- Using a draft scorecard as runtime truth
- LLM-generated metric catalogs (v1 is deterministic)
- Cross-tenant scorecard learning
- Full Scout / campaign evaluation rewiring (runtime helper is exported; consumers land in a later slice)
- Live metric collection dashboards
- Changing Outcome Intelligence so it rewrites reasoning (ADR-008 remains in force)

## Inputs

- Operator objectives
- Business Blueprint / Client Intelligence
- Published AIM
- Business stage
- Historical outcomes
- Business model / revenue model
- Operator preferences and prior scorecard feedback

## Reasoning Pipeline

```text
Business Understanding
        ↓
Business Objectives
        ↓
Business Stage
        ↓
Business Model
        ↓
Outcome Intelligence
        ↓
Draft Operator Scorecard
```

The Draft Scorecard is generated by Max. It is never used for reporting.

## Operator Review

Available actions:

- Accept
- Modify
- Add
- Remove
- Reorder

If a recommendation is removed, Max may ask:

> I noticed you removed "Pain Confirmation Rate." Would you like to tell me why?

Providing a reason is optional. Feedback becomes Operator Intelligence.

## Runtime

```text
Draft Scorecard → Operator Review → Approved Scorecard
        ↓
Executive Business Brief · Daily Briefings · Outcome Intelligence
```

Every PulseForge component that reports success references the approved scorecard rather than Max's draft recommendations.

## Scorecard Categories

Acquisition · Market Validation · Sales · Delivery · Business Outcomes

Profile examples (Babrun, Anchor Cleaning) map Commercial / Operations / Transformation onto this catalog without inventing metrics the operator did not earn.

## Executive Business Brief

Replace **Success Looks Like** with:

1. **Recommended Operator Scorecard** — Max recommends these metrics because they best support the operator's stated business objectives.
2. **Operator Approved Scorecard** — The following metrics have been explicitly approved by the operator and define business success.
3. **Metrics Under Review** — Metrics Max recommends but the operator has not yet accepted.

## Learning Loop

Every scorecard modification becomes learning.

Examples:

- Operator removed Pain Confirmation Rate because it is already validated → future recommendations adjust.
- Operator added Referral Partners Created → future briefings prioritize this metric.

## Scorecard Evolution

Businesses evolve. The scorecard should evolve with them.

Max periodically evaluates whether the current scorecard still reflects the business. Example:

> Your business has transitioned from market validation to operational scale. I'd recommend replacing "Pain Confirmation Rate" with "Student Completion Rate."

Operators review the proposed changes. Nothing changes automatically.

## Governance

Recommendations are advisory. The operator-approved scorecard is authoritative.

Daily Briefings, Executive Business Briefs, business health assessments, Outcome Intelligence, Scout prioritization, campaign evaluation, and strategic recommendations must reference the approved scorecard once one exists.

## Architecture

```text
Client Intelligence / Blueprint / AIM / Objectives / Outcomes
        ↓
packages/operator-scorecard (reason → draft → review → approve → learn → evolve)
        ↓
Approved scorecard (tenant-scoped business intelligence)
        ↓
Brief · Daily briefing · Max digest · Runtime consumers
```

v1 reasoning is deterministic. No LLM invents metrics. Profile catalogs (founder transformation, commercial cleaning, default) plus operator-stated success metrics produce the draft.

## Data Model

Tables: `operator_scorecards`, `operator_scorecard_learning`.

`operator_scorecards.payload` holds the versioned scorecard document (metrics, reasoning trace, reviews). Status is `draft` | `in_review` | `approved` | `superseded`.

Learning rows capture accept / modify / remove / add / reorder with optional reason. They never rewrite prior drafts.

Tenant isolation: `tenant_id` / `client_id`. Cross-tenant reads fail closed.

## Implementation Plan

1. Spec + ADR-053 + registry
2. `packages/operator-scorecard` reasoning, review, approval, learning, evolution
3. Persistence + routes + `/operator-scorecard` review UI
4. CIE Executive Business Brief sections
5. Daily briefing + Max digest consume approved scorecard only
6. Tests + competency

## Migration Strategy

Additive. `migrations/2026-08-19-operator-scorecard-intelligence.sql` plus rollback. Existing tenants start with no approved scorecard. Draft generation is on demand.

## Testing

- `packages/operator-scorecard/tests/osi.test.js` — pipeline, explainability, review, approval, learning, evolution, tenant isolation, draft-not-runtime
- `test/operatorScorecard.test.js` — service, routes, Brief sections, competency
- CIE brief tests updated for Recommended / Approved / Under Review

## Acceptance Criteria

- [x] Max reasons from business objectives before recommending metrics
- [x] Every recommendation includes supporting reasoning
- [x] Operators can accept, modify, remove, reorder, or add metrics
- [x] Operator feedback becomes learning
- [x] The approved scorecard becomes tenant-scoped business intelligence
- [x] Executive Business Briefs distinguish between Max recommendations and operator-approved metrics
- [x] Daily Briefings use the approved scorecard as the definition of business success
- [x] Max periodically recommends scorecard updates as the business matures
- [x] Drafts are never used for reporting

## Future Work

- Scout prioritization and campaign evaluation consume the approved scorecard as a first-class input
- LLM-polished recommendation copy over the deterministic catalog
- Live metric measurement against the approved scorecard
- Operator-facing evolution conversation in Max chat

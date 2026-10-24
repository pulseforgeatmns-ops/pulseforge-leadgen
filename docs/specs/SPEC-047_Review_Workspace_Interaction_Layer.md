# SPEC-047 — Review Workspace Interaction Layer

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-034, SPEC-042, SPEC-045, ADR-030, ADR-031 |
| **Consumed by** | Mission Workspace, Command Deck Review Experience |
| **Numbering note** | Draft titled SPEC-046; **SPEC-046** is already [Trade Intelligence Engine](SPEC-046_Trade_Intelligence_Engine.md). This contract is **SPEC-047**. |

## Objective

Complete the operator review experience by wiring existing Mission / Artifact review payloads to the Command Deck. Operators inspect generated work, evidence, and warnings without opening Developer Details or using developer tools.

## Guiding principle

**Never display information that cannot be inspected.** Every count shown to an operator must drill into the underlying business records.

## Vision References

- [ADR-031](../adr/ADR-031_Review_Must_Be_Evidence_First.md) — Review must be evidence-first
- [ADR-030](../adr/ADR-030_Command_Deck_Is_an_Operator_Workspace.md) — Command Deck is an operator workspace
- [SPEC-034](SPEC-034_Campaign_Review_Workspace.md) — Campaign Review Workspace (capability view model)
- [SPEC-045](SPEC-045_Command_Deck_UX_Polish.md) — Command Deck UX Polish (summaries without interaction)

## Problem

SPEC-045 added Campaign Summary metrics, deliverable cards, and stage chrome that *look* reviewable but are mostly static. Operators cannot expand packages, inspect warnings, or preview letters without Developer Details JSON.

## Scope

### In scope (v1)

Presentation and interaction only on Mission Workspace Review surfaces:

1. Expandable Campaign / MailPackage deliverable cards → package list
2. Expandable stage cards → artifacts, evidence summary, warnings, blocking issues
3. Review summary metrics navigate to underlying records
4. Warning inspector (list + open related package)
5. Mail package preview (recipient, company, confidence, personalization, letter)
6. Developer Details remain collapsed, last, optional
7. Honest affordance (interactive vs static)
8. Review queue — one package at a time (Approve / Edit / Previous / Next); local session progress

### Out of scope

- Mission Engine, Artifact Bus, or Campaign Review capability execute-path changes
- Persisting per-package approve/edit to the Campaign Review store (session-local UI progress in v1; mission-level Approve remains the dock action)
- Envelope preview chrome beyond existing payload fields (future)
- New API contracts

## Architecture

```text
GET /api/v1/missions/:id workspace payload
      ↓
Command Deck normalizes packages / warnings / stages (client)
      ↓
Review UI: Summary → Queue → Packages → Stages → Developer Details
```

Summaries and lists are derived from existing `artifacts`, `plan.steps`, `evidence`, `stageReview`, and `deliverables` fields.

## Acceptance Criteria

- [x] Campaign cards expand
- [x] Stage cards expand
- [x] Summary metrics navigate
- [x] Warnings are inspectable
- [x] Mail packages are reviewable
- [x] Generated letters are accessible without Developer Details
- [x] Developer Details remain optional / collapsed by default
- [x] No static element appears interactive
- [x] No raw JSON is required for normal operator review

## Testing

- Manual: completed Mission with Campaign + MailPackage — expand cards, navigate metrics, open warning → package, walk review queue, confirm Developer Details optional
- No MissionEngine test changes required (UI-only)

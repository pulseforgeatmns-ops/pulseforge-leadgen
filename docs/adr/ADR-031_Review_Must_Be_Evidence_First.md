# ADR-031 — Review Must Be Evidence-First

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-047](../specs/SPEC-047_Review_Workspace_Interaction_Layer.md) |
| **Supersedes** | — |
| **Related** | [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-030](ADR-030_Command_Deck_Is_an_Operator_Workspace.md), [SPEC-034](../specs/SPEC-034_Campaign_Review_Workspace.md), [SPEC-045](../specs/SPEC-045_Command_Deck_UX_Polish.md) |

## Context

Mission OS generates campaigns, mail packages, evidence, and validation warnings. The Mission Workspace Review surface still pushed operators toward artifact metadata and JSON payloads for ordinary approval decisions. Counts were shown without drill-down. That violates operator trust: information that cannot be inspected should not be displayed as a decision aid.

## Decision

1. **The Review Workspace shall present business artifacts and evidence before implementation metadata.**
2. **Approval decisions** are made from generated work (letters / packages), supporting evidence, and actionable warnings — **not** artifact schemas or JSON payloads.
3. **Implementation details** (revisions, provenance, dependencies, raw payloads, audit dumps) remain available under Developer Details, collapsed by default, and never shown first.
4. **Every operator-facing count must navigate or expand into the underlying business records.**
5. **UX under this ADR must not alter Mission execution, Artifact Bus, or Campaign Review capability behavior.** Presentation derives from existing workspace payloads.
6. Implementing contract: [SPEC-047 Review Workspace Interaction Layer](../specs/SPEC-047_Review_Workspace_Interaction_Layer.md).

## Rationale

Mission OS is an operator system. Human approval before execution ([ADR-021](ADR-021_Human_Approval_Before_Execution.md)) is only meaningful when operators can inspect the work product. Evidence-first review reduces cognitive load and prevents “approve the JSON” failure modes.

## Consequences

### Positive

- Letters and packages are reviewable without Developer Details
- Warnings are actionable and linked to packages
- Fake click affordances are removed from static chrome

### Negative / tradeoffs

- Client-side normalization of heterogeneous payloads (Campaign `mailMerge` vs MailPackage `packages`) may need maintenance as producers evolve
- Session-local package Approve/Edit progress does not yet persist to SPEC-034 store (follow-up)

### Follow-ups

- [x] SPEC-047 v1 interaction layer on Command Deck Mission Workspace
- [ ] Persist per-package review actions via existing Campaign Review capability APIs when UI binding lands
- [ ] Envelope preview chrome beyond text fields already on the package

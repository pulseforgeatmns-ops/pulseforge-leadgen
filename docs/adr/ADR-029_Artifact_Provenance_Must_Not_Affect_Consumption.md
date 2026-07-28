# ADR-029 — Artifact Provenance Must Not Affect Consumption

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-043](../specs/SPEC-043_Operator_Artifact_Injection.md) |
| **Supersedes** | — |
| **Related** | [ADR-028](ADR-028_Business_State_Flows_Through_Artifacts.md), [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md), [ADR-022](ADR-022_Execution_Consumes_Approved_Artifacts.md), [SPEC-042](../specs/SPEC-042_Mission_Artifact_Bus.md) |

## Context

The Mission Artifact Bus (SPEC-042 / ADR-028) makes typed, validated artifacts the canonical business state. Discovery is the primary producer of `ProspectList` today, but operator-assisted workflows (CSV import, CRM paste, purchased lists) must also publish the same artifact type when Discovery cannot or should not run.

If consumers branched on producer identity (`prospect_discovery` vs `operator_manual`), the pipeline would accumulate capability-specific conditionals, break deterministic resume/replay, and treat equivalent business state as different execution paths.

## Decision

1. **Artifact consumers shall resolve artifacts solely by:**
   - Artifact type
   - Validation status
   - Revision
2. **Producer identity shall be treated as provenance only** and shall not alter execution behavior.
3. **Operator-injected artifacts are first-class citizens** of the Artifact Bus once validated.
4. Implementing contract: [SPEC-043 Operator Artifact Injection](../specs/SPEC-043_Operator_Artifact_Injection.md).

## Rationale

A ProspectList produced by Discovery, imported from a CRM, pasted by an operator, or created through a future integration represents the same business artifact once validated. Maintaining this separation:

- Preserves deterministic execution
- Enables operator-assisted automation
- Simplifies future integrations
- Prevents capability-specific branching throughout the Mission pipeline

## Consequences

### Positive

- Company Intelligence and downstream stages stay origin-agnostic
- Operator ingress reuses Pipeline Gate / registry validation without parallel schemas
- Audit and Workspace can still show how an artifact was created

### Negative / tradeoffs

- Operators can advance past Discovery without Places success — intentional; requires Workspace recovery UX and clear provenance
- Stronger field validation on ProspectList may surface warnings for thin Discovery rows (company name remains required)

### Follow-ups

- [x] Implement SPEC-043 thin slice (inject API, Discovery Satisfied Operator Supplied, Workspace Import)
- [ ] Future CRM / API ingress adapters on the same normalize → validate → publish path

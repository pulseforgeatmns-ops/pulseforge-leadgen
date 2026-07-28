# ADR-036 — Trust Through Contracts

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-052](../specs/SPEC-052_Typed_Artifact_Validation.md) |
| **Related** | [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md), [ADR-028](ADR-028_Business_State_Flows_Through_Artifacts.md), [ADR-029](ADR-029_Artifact_Provenance_Must_Not_Affect_Consumption.md), [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-035](ADR-035_Plan_Around_State_Not_Sequence.md) |

## Context

Mission Planning (SPEC-050) correctly separates operator intent. Artifact Resolution (SPEC-051) correctly resolves dependencies. The Artifact Bus (SPEC-042) and Pipeline Gate (SPEC-040) validate business completeness before advance.

However, arbitrary text could still enter execution as structured artifacts — for example mission prose (“Reuse existing ProspectList…”) being parsed as ProspectList company rows. Natural language is not an artifact. Downstream capabilities must not re-verify trust on every consume.

## Decision

1. **Artifacts shall be validated at system boundaries** rather than within downstream capabilities.
2. **Validation belongs where information changes from untyped input into trusted platform state.**
3. **Capabilities consume artifacts.** They do not verify them.
4. **Trust is established once.** Never repeatedly.
5. **Only validated artifacts may enter the Artifact Bus** as consumable revisions. Failures remain reviewable plain text / quarantine — never executable contracts.
6. Implementing contract: [SPEC-052 Typed Artifact Validation](../specs/SPEC-052_Typed_Artifact_Validation.md).

## Consequences

### Positive

- Downstream systems may assume “I am receiving the artifact I requested”
- Natural language, notes, and LLM explanations cannot become structured execution inputs
- Single validation authority (Artifact Validator) at the boundary
- Failures are operator-visible without polluting the bus

### Negative / tradeoffs

- Stricter ingress may reject ambiguous pastes that look like prose (fail closed; operator re-imports as CSV)
- Semantic rules for ProspectList company names are heuristic (company suffixes / instruction patterns)

### Follow-ups

- [x] ArtifactValidator pipeline + ProspectList NL rejection (SPEC-052 v1)
- [x] Artifact Bus publishes only after typed validation
- [x] Review Workspace Artifact Validation failure surface
- [ ] Per-type schema version migration tooling
- [ ] Operator-editable quarantine → revalidate flow

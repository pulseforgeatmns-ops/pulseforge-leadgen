# ADR-026 — Business Success Determines Pipeline Progress

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-040](../specs/SPEC-040_Mission_Artifact_Validation.md) |
| **Supersedes** | — |
| **Related** | [ADR-002](ADR-002_Explainable_AI.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-017](ADR-017_Intelligence_Before_Execution.md), [ADR-021](ADR-021_Human_Approval_Before_Execution.md), [ADR-022](ADR-022_Execution_Consumes_Approved_Artifacts.md), [ADR-025](ADR-025_Active_Missions_Take_Precedence.md), [SPEC-024](../specs/SPEC-024_Prospect_Discovery_Capability.md), [SPEC-032](../specs/SPEC-032_Mission_Memory.md), [SPEC-039](../specs/SPEC-039_Active_Mission_Resolver.md) |

## Context

The current pipeline measures technical execution rather than business outcomes.

This allows stages to complete while producing empty or unusable artifacts, reducing operator trust and propagating invalid state through the Mission. Example: Discovery reports **Completed** with Discovered/Verified/Rejected all `0`, then enrichment and campaign stages still run on empty inputs.

[ADR-011](ADR-011_Capability_Framework.md) treats capabilities as the stable API, but CapabilityResult `completed` today means “execute() returned,” not “business contract satisfied.” [ADR-021](ADR-021_Human_Approval_Before_Execution.md) gates outreach approval; it does not stop mid-pipeline empty discovery from advancing.

## Decision

1. **Pipeline progression is governed by validated business artifacts.**
2. **A stage advances only after publishing artifacts that satisfy its declared business contract.**
3. **Stages that execute successfully but fail business validation** enter **Completed With Warnings** or **Blocked**, depending on severity — not a false **Completed**.
4. **Technical execution alone is not sufficient to advance the Mission.**
5. **Downstream stages consume validated (published) artifacts only.** Quarantined artifacts are never inputs to later capabilities.
6. **Discovery Profile resolution is deterministic** with strict precedence (constraints → operator override → pinned client profile → client default geography → mission-type default) and never silently switches geography when a client profile exists.
7. Implementing contract: [SPEC-040 Mission Artifact Validation & Discovery Resolution](../specs/SPEC-040_Mission_Artifact_Validation.md).

## Consequences

### Positive

- Mission progress reflects real business value rather than code execution
- Operators receive clear explanations for blocked workflows
- Downstream stages never consume invalid or empty artifacts
- Mission evidence becomes a trustworthy audit trail of both technical execution and business outcomes

### Negative / tradeoffs

- More Missions pause in `waiting` when discovery yields zero — intentional; operators must act
- Stage contracts must stay aligned with capability output shapes
- Shortfalls (e.g. 17 of 20) continue with warnings; threshold tuning may be needed per client

### Follow-ups

- [x] Implement SPEC-040 thin slice (resolver report, contracts, pipeline gate, executor wiring)
- [x] Surface stage outcomes in Mission Workspace / Operations (blocking issues + resolution fields)
- [ ] Metrics: discovery success rate, yield, validation failures, stop locations (audit payloads in v1)
- Update CURRENT_STATE when Artifact Validation ships — done

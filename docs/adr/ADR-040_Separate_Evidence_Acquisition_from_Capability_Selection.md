# ADR-040 — Separate Evidence Acquisition from Capability Selection

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-056](../specs/SPEC-056_Evidence_Driven_Capability_Planning.md) |
| **Related** | [ADR-039](ADR-039_Separate_Understanding_from_Execution.md), [ADR-038](ADR-038_Explain_Planning_Decisions.md), [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-010](ADR-010_Mission_Engine.md) |

## Context

SPEC-055 introduced Intent Understanding so operators could ask questions like “Why did Campaign 001 fail?” without memorizing capability aliases. Capability Planning then mapped Campaign Diagnostics directly to Campaign Review and Outcome Intelligence.

Those capabilities consume existing artifacts. When Discovery produced zero verified companies and never emitted diagnostic evidence, review capabilities had nothing to explain — and still reported as if a review were meaningful.

Intent answers *what the operator wants*. It does not answer *what must be known* before a capability can honestly respond.

## Decision

1. **Mission Planning includes three distinct phases:**
   - **Intent Understanding** — language → MissionIntent
   - **Evidence Planning** — MissionIntent → EvidencePlan (required vs available vs missing)
   - **Capability Planning** — EvidencePlan + MissionIntent → MissionPlan
2. **MissionIntent declares `requiresEvidence`** descriptively; it does not select capabilities.
3. **Evidence Planning compares requirements against the artifact catalog** and resolves producers from the Capability Registry.
4. **Missing evidence schedules diagnostic (or other) producers before downstream review/outcome capabilities.**
5. **If required evidence cannot be acquired**, the planner reports `Unable to answer` with missing types and reasons — it does not invent incomplete diagnostic narratives.
6. **Diagnostic capabilities are read-only** and produce typed diagnostic artifacts; they never mutate business state.
7. Implementing contract: [SPEC-056 Evidence-Driven Capability Planning](../specs/SPEC-056_Evidence_Driven_Capability_Planning.md).

## Consequences

### Positive

- Diagnostic questions acquire the evidence needed to answer them
- Review capabilities are not scheduled as a substitute for missing diagnostics
- Operators see Evidence Requirements in Review Workspace
- Clear boundary: language / questions / execution / work

### Negative / tradeoffs

- Intent categories must maintain an evidence-requirement map
- Diagnostic producers must be registered or questions fail closed (by design)
- Evidence catalog availability depends on workspace / prior-mission artifact injection

### Follow-ups

- [x] MissionIntent.requiresEvidence + EvidencePlan + EvidencePlanner (SPEC-056 v1)
- [x] Discovery Diagnostics capability + diagnostic artifact types
- [x] CapabilityPlanner evidence-aware stage merge
- [x] Review Workspace Evidence Requirements panel
- [ ] Historical Discovery log replay across environments
- [ ] Campaign Diagnostics capability distinct from Discovery Diagnostics
- [ ] Interactive blocked-evidence remediation in Command Deck

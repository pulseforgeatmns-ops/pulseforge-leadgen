# ADR-028 — Business State Flows Through Artifacts

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-042](../specs/SPEC-042_Mission_Artifact_Bus.md) |
| **Supersedes** | Flat `priorOutputs` merge as the sole inter-stage data contract |
| **Related** | [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-022](ADR-022_Execution_Consumes_Approved_Artifacts.md), [ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md), [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [SPEC-040](../specs/SPEC-040_Mission_Artifact_Validation.md), [SPEC-041](../specs/SPEC-041_Mission_Planner.md) |

## Context

Capabilities currently exchange implementation-specific objects, tightly coupling stages and limiting replay, inspection, versioning, and caching.

Mission execution should be driven by durable business artifacts rather than transient function return values.

[ADR-026](ADR-026_Business_Success_Determines_Pipeline_Progress.md) established that only validated artifacts advance the pipeline. [SPEC-040](../specs/SPEC-040_Mission_Artifact_Validation.md) stamps and quarantines artifact descriptors on CapabilityResults, but inter-stage data still flows through a flat `priorOutputs` object merge. That merge is not typed, versioned, or replayable.

## Decision

1. **The Mission Artifact Bus becomes the canonical mechanism for exchanging business state.**
2. **Capabilities publish immutable, typed artifacts to the bus.**
3. **Downstream stages consume validated artifacts through the Mission Engine** rather than directly from upstream capability outputs.
4. **Artifacts are immutable and versioned.** Updates publish a new revision; prior revisions remain for history, comparison, and replay.
5. **Quarantined artifacts are invisible to consumers.** Pipeline Gate validation remains the gate; the bus enforces consumption rules.
6. **Mission Planner plans stages; Mission Executor runs stages; Artifact Bus carries business state.** Each subsystem has a single responsibility.
7. Implementing contract: [SPEC-042 Mission Artifact Bus](../specs/SPEC-042_Mission_Artifact_Bus.md).

## Consequences

### Positive

- Business state becomes durable, inspectable, and replayable
- Stage coupling is significantly reduced
- Replay, comparison, caching, and provenance become native capabilities
- Future features (branching workflows, parallel execution, Outcome Intelligence) can build on a stable artifact model without changing existing capabilities

### Negative / tradeoffs

- Capabilities still emit free-form `outputs` in v1; the Mission Engine adapts them into typed artifacts (full capability migration is follow-up)
- Artifact snapshots live in `missions.deliverables` JSON for v1 — dedicated artifact tables deferred (aligns with SPEC-032 Mission Memory)
- Stage Library snake_case produces/consumes names remain aliases of registry types

### Follow-ups

- [x] Implement SPEC-042 thin slice (Artifact Registry, Artifact Bus, executor publish/consume, workspace Artifacts surface)
- [ ] Dedicated durable artifact store / Mission Memory integration (SPEC-032)
- [ ] Capabilities publish via `publishArtifact` directly (retire output-adapter path)
- Update CURRENT_STATE when Artifact Bus ships — done

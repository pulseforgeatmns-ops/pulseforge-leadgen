# ADR-034 — Intent Before Execution

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-28 |
| **Spec** | [SPEC-050](../specs/SPEC-050_Deterministic_Mission_Planning.md) |
| **Related** | [ADR-027](ADR-027_Mission_Planning_Is_Objective_Driven.md), [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md) |

## Context

SPEC-041 made Mission Planning objective-driven, but free-form operator language could still influence stage selection (e.g. bare “review” matching Campaign Review) and flow into capability `context.objective` as raw text. That leaked conversational guidance into runtime artifacts and produced incorrect execution graphs even when the pipeline “succeeded.”

## Decision

1. **Mission Planning compiles operator intent into a Mission Plan IR before any capability executes.**
2. **Natural language is an interface.** The Mission Plan is the executable contract.
3. **Capabilities consume only the Mission Plan** (structured objective, parameters, options). They never consume raw operator Notes or unparsed prompt text for business fields.
4. **Unknown capability requests become Notes** — never new runtime nodes.
5. **Reserved runtime fields** (company, recipient, decision maker, artifact/package/stage names, capability) may only originate from runtime artifacts.
6. Implementing contract: [SPEC-050 Deterministic Mission Planning](../specs/SPEC-050_Deterministic_Mission_Planning.md).

## Consequences

### Positive

- Prevents prompt leakage into letters, packages, and metadata
- Makes execution graphs deterministic and reviewable
- Enables validation before work begins
- Improves debugging (Notes vs Execution are explicit)
- Downstream capabilities receive structured inputs

### Negative / tradeoffs

- Deterministic grammar will miss some free-form phrasings (fail closed to Notes)
- Legacy missions without `missionPlan` still pass `objectiveText` as fallback
- Operators must learn that guidance belongs in Notes, not as implied stages

### Follow-ups

- [x] Intent Parser + Mission Plan IR + planner/executor wiring (SPEC-050 v1)
- [x] Review Workspace Mission Plan panel
- [ ] Interactive approve/edit of Mission Plan before run
- [ ] Stronger write-path leak assertions in mail / SI generators

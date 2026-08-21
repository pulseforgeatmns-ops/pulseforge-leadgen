# ADR-056 — Mission Planning Engine Is The Single Interpreter

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-21 |
| **Spec** | [SPEC-130](../specs/SPEC-130_Structured_Mission_Planning.md) |
| **Related** | [ADR-034](ADR-034_Intent_Before_Execution.md), [ADR-039](ADR-039_Separate_Understanding_from_Execution.md), [ADR-055](ADR-055_Max_Manages_Missions.md), [ADR-003](ADR-003_Human_Approval.md) |

## Context

Acquisition missions previously left each specialist to interpret operator English. Scout inferred market and geography. Paige inferred audience. Vera inferred market. Rex parsed objectives. That guarantees drift.

SPEC-130 introduced a structured mission contract. This ADR records the stronger rule: **one interpreter**.

## Decision

1. The Mission Planning Engine is the only component that interprets operator natural language into mission fields.
2. The approved Mission Plan is the canonical contract for the lifetime of the mission.
3. Specialists receive structured contracts only. They never parse operator English.
4. Ambiguous language is a question, not a guess.
5. Blueprint, workspace state, historical memory, and general knowledge may inform missing fields. They may not override operator approval or the locked Mission Plan.
6. Mission Planning never executes.

## Consequences

### Positive

- Scout, Paige, Vera, Rex, and future specialists see identical mission data
- Operator confirmation is explicit (Approve / Edit / Cancel)
- Interpretation is explainable: every field carries provenance

### Negative / tradeoffs

- Underspecified objectives pause for clarification before Discovery
- Existing “Manchester” shorthand now asks NH vs UK unless the operator (or an already-locked plan) disambiguates

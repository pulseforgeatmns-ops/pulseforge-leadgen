# SPEC-050 — Deterministic Mission Planning

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-041, SPEC-023, SPEC-022, SPEC-047; ADR-027 |
| **ADR** | [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md) |

## Objective

Separate operator intent from executable workflow.

Mission Planning shall translate natural language into a deterministic execution graph.
Operator instructions, explanations, and conversational language shall **never** become executable capability nodes or runtime data.

## Vision References

- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [ADR-027 Mission Planning Is Objective-Driven](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md)
- [SPEC-041 Mission Planner](SPEC-041_Mission_Planner.md)
- [SPEC-047 Review Workspace Interaction Layer](SPEC-047_Review_Workspace_Interaction_Layer.md)
- `docs/vision/Mission.md`

## Problem

Current Mission Planning treated portions of free-form operator instructions as executable workflow.

Example operator prompt:

> Build Campaign 001 for Anchor Cleaning using the current ProspectList. Execute the complete pipeline through Sales Intelligence. Review Human Test results and generated letters.

Incorrect behavior created unintended nodes (e.g. “Review Human Test…”, “Generated Letters…”) that propagated into Review Workspace, package metadata, company/recipient names, and generated copy (“Dear and generated letters…”).

The pipeline could still run. The **execution graph was wrong**.

## Design Principle

Natural language is expressive. Execution graphs are deterministic. Mission Planning translates one into the other.

```text
Operator Objective
        │
        ▼
Mission Planner
        │
        ▼
Intent Parser
        │
        ▼
Mission Plan (IR)
        │
        ▼
Capability Graph
        │
        ▼
Execution
```

Only the Mission Plan may create executable nodes.

## Scope (v1)

- `packages/mission-engine/IntentParser.js` — sentence classification (Objective / Parameters / Execution / Options / Notes)
- `packages/mission-engine/MissionPlan.js` — Mission Plan IR, capability resolution, validation, leak detection
- `MissionPlanner.plan` compiles NL → Mission Plan before `createExecutionGraph`
- `ExecutionGraph` keyword matching uses only executable Mission Plan text (never Notes)
- `MissionExecutor` passes Mission Plan objective to capabilities (ADR-034)
- Review Workspace displays parsed Mission Plan (`command-deck.js`)
- Stage Library: tighten Review / intelligence patterns so guidance text cannot select stages

## Out of Scope

- LLM-based parsing (v1 is deterministic regex / grammar)
- Durable Mission Plan edit UI (approve/edit affordances beyond display)
- Rewriting all capability stubs to refuse `context.objectiveText` legacy field
- SPEC-032 Mission Memory persistence of plan revisions

## Mission Grammar

Every sentence maps to exactly one category:

| Category | Role | Example |
|---|---|---|
| Objective | Primary business goal | Build Campaign 001 |
| Parameters | Structured inputs | ProspectList: current; client/subject |
| Execution | Registered capability | Campaign Builder; Sales Intelligence |
| Options | Execution modifiers | Review.; Dry Run; Ready to Print |
| Notes | Operator guidance | Review Human Test results |

**Notes never execute.** Unknown capability text becomes a Note.

## Reserved Runtime Fields

May only originate from runtime artifacts — never operator language:

Company · Recipient · Capability · Artifact Name · Package Name · Stage Name · Decision Maker

## Validation

Before execution:

- Every executable node maps to a registered capability / stage
- Parameters use known schemas (unknown → warning)
- Unknown text classified as Notes
- Reserved runtime fields remain protected
- Graph validation still fail-closed (SPEC-041)

## Acceptance Criteria

- [x] Free-form operator language never becomes executable nodes
- [x] Every execution node maps to a registered capability
- [x] Unknown instructions become Notes
- [x] Mission Plans validate before execution
- [x] Reserved runtime fields cannot be populated from operator text
- [x] Generated artifacts must not contain fragments of operator Notes (leak helper + executor isolation)
- [x] Review Workspace displays the parsed Mission Plan
- [x] Operators can see Review / Notes before treating guidance as work (plan approval flags)

## Testing

```bash
npm run test:mission
```

Primary coverage: `packages/mission-engine/tests/deterministicMissionPlan.test.js`

## Future Work

- Interactive Mission Plan editor (approve / edit / recompile)
- Stronger artifact-level leak guards inside Mail Package / Sales Intelligence writers
- Persist Mission Plan revisions with SPEC-032 Mission Memory

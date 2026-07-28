# SPEC-055 — Intent Understanding

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-041, SPEC-050, SPEC-051, SPEC-054; ADR-034, ADR-038 |
| **ADR** | [ADR-039 Separate Understanding from Execution](../adr/ADR-039_Separate_Understanding_from_Execution.md) |

## Objective

Operators communicate goals. Pulseforge determines execution.

Mission Planning shall understand operator intent semantically before selecting deterministic capabilities.

## Vision References

- [ADR-039 Separate Understanding from Execution](../adr/ADR-039_Separate_Understanding_from_Execution.md)
- [ADR-034 Intent Before Execution](../adr/ADR-034_Intent_Before_Execution.md)
- [ADR-038 Explain Planning Decisions](../adr/ADR-038_Explain_Planning_Decisions.md)
- [ADR-027 Mission Planning Is Objective-Driven](../adr/ADR-027_Mission_Planning_Is_Objective_Driven.md)
- [SPEC-050 Deterministic Mission Planning](SPEC-050_Deterministic_Mission_Planning.md)
- [SPEC-054 Capability Registry & Planner Diagnostics](SPEC-054_Capability_Registry_and_Planner_Diagnostics.md)

## Problem

Current Mission Planning resolved execution through capability aliases.

Example:

> Operator: "Run an end-to-end execution audit for Campaign 001."
>
> Planner: Unknown mission alias.

The operator communicated valid intent. The planner failed because it searched for matching commands instead of understanding the request.

## Design Principle

Humans express intent. The platform plans execution. Operators should never need to memorize internal capability names.

## Two-Stage Planning

```text
Operator
  ↓
Intent Understanding
  ↓
MissionIntent
  ↓
Capability Planning
  ↓
MissionPlan
  ↓
Execution Graph
```

| Artifact | Role |
|---|---|
| **MissionIntent** | Descriptive — what the operator wants |
| **MissionPlan** | Executable — which registered capabilities run |

## MissionIntent

Intermediate artifact fields:

| Field | Example |
|---|---|
| `goal` | Run Campaign |
| `domain` | Direct Mail |
| `mode` | Execution |
| `target` | Campaign 001 |
| `constraints` | Reuse existing artifacts |
| `diagnostics` | true |
| `confidence` | 0.96 |
| `matchedIntent` | campaign_execution |
| `alternateIntents` | […] |

## Intent Understanding

The planner answers: **What is the operator trying to accomplish?**

Not: Which capability name appears in the sentence?

## Capability Planning

MissionIntent becomes deterministic planning:

```text
Goal: Run Campaign
  ↓
Required Capability: Direct Mail Execution
  ↓
Execution Graph
```

## Intent Categories

These are intents — not capabilities:

- Campaign Execution
- Campaign Review
- Campaign Creation
- Campaign Diagnostics
- Discovery Investigation
- Prospect Discovery
- Generate Messaging
- Build Business Intelligence
- Review Prospect
- Generate Proposal
- Mail Package Generation
- Export Campaign
- Import Prospect List
- Outcome Intelligence
- Operator Inbox
- Operator Help
- Diagnostics

## Semantic Resolution Examples

| Operator | Intent | Execution |
|---|---|---|
| "Why isn't Discovery finding anyone?" | Discovery Investigation | Discovery → BI → Review |
| "Let's see what's wrong with Campaign 001." | Campaign Diagnostics | Campaign Review → Outcome Intelligence |
| "Run the campaign." | Campaign Execution | Direct Mail Execution |
| "Run an end-to-end execution audit for Campaign 001." | Campaign Diagnostics | Campaign Review → Outcome Intelligence |

## Capability Independence

| Layer | Owns |
|---|---|
| Intent Understanding | Language |
| Capability Planning | Execution selection |
| Capabilities | Work |

Capabilities never parse language. They consume MissionPlan.

## Confidence

Intent Understanding produces:

- `confidence`
- `matchedIntent`
- `alternateIntents`

High confidence (≥ 0.75) proceeds automatically. Low confidence triggers clarification with suggested interpretations.

## Review Workspace

Mission section surfaces:

1. **Operator Request** — raw text
2. **Understood Intent** — category + confidence + alternatives
3. **Execution Plan** — deterministic capabilities from MissionIntent

## Scope (v1)

- `packages/mission-engine/MissionIntent.js` — MissionIntent IR
- `packages/mission-engine/IntentUnderstanding.js` — semantic category matching + confidence
- `packages/mission-engine/CapabilityPlanner.js` — intent → MissionPlan map
- `MissionPlanner.plan` runs Understanding → Planning before Execution Graph
- Clarification draft when confidence is low
- Review Workspace Understood Intent panel (`command-deck.js`)
- Multi-sentence campaign builds still use IntentParser for Notes/Options (SPEC-050 compat)

## Out of Scope

- LLM-based intent classification (v1 is deterministic semantic patterns)
- Interactive clarification UI beyond suggested interpretations payload
- Retiring IntentParser alias path entirely
- New Diagnostics capability module (diagnostics mode reuses Review / Outcome / Discovery)

## Acceptance Criteria

- [x] Operators never need to know capability names
- [x] Free-form requests resolve into MissionIntent
- [x] MissionIntent resolves deterministically into MissionPlan
- [x] Capabilities never parse natural language
- [x] Confidence is surfaced
- [x] Ambiguous requests present suggested interpretations
- [x] Existing deterministic execution remains unchanged (SPEC-050 tests pass)

## Tests

`npm run test:mission` — `intentUnderstanding.test.js` (+ existing deterministic / diagnostics suites)

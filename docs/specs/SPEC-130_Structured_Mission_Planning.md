# SPEC-130 — Structured Mission Planning

**Status:** Implemented  
**Depends on:** SPEC-118 (Acquisition Mission Orchestration), SPEC-128 (Operator Approval)

## Purpose

Introduce a dedicated Mission Planning stage that converts operator intent into a structured Acquisition Mission **before** any specialist executes. Scout, Paige, Vera, Rex, and future specialists consume the structured mission — not free-form operator text.

## Philosophy

- The mission plan is the **canonical representation** of operator intent.
- Specialists must **never infer** business objectives independently.
- **Mission Planning owns interpretation.** Specialists own execution.

## Flow

```
Operator objective (NL)
        ↓
   Mission Planner
        ↓
 Structured Mission (draft)
        ↓
 Operator confirms plan
        ↓
 Structured Mission (immutable)
        ↓
 Scout · Paige · Vera · Rex
```

## Structured Mission Contract

```json
{
  "missionType": "acquisition",
  "objective": "Acquire one recurring commercial cleaning client.",
  "successMetric": { "type": "customers", "target": 1 },
  "market": {
    "segment": "short_term_rental",
    "industry": "hospitality",
    "buyer": "property_operator"
  },
  "geography": {
    "region": "Greater Manchester",
    "cities": ["Manchester", "Hooksett", "Bedford", "Auburn", "Goffstown"]
  },
  "constraints": ["recurring", "commercial_only"],
  "priority": 1
}
```

After operator approval the contract is frozen with `immutable: true` and a `contractHash`.

## Operator Confirmation

1. Max creates mission and shows **Mission Understanding** (objective, market, region, buyer, constraints).
2. Operator approves or edits the plan (`Approve mission plan?`).
3. Plan becomes immutable; pending decision advances to `Approve discovery?`.
4. Only after discovery approval does Scout execute — using structured fields only.

## Specialist Input Contracts

| Specialist | Receives | Must not infer |
|---|---|---|
| Scout | segment, industry, buyer, geography, constraints, successMetric | market, region |
| Paige | market, buyer, objective, campaignGoal, constraints | target audience |
| Vera | market, buyer, region, companies | market |
| Rex | mission, objective, successMetric, progress | NL parsing |

## Implementation

| Module | Role |
|---|---|
| `packages/acquisition-mission/MissionPlanner.js` | NL → structured contract |
| `packages/acquisition-mission/StructuredMission.js` | Schema, validation, freeze, display |
| `packages/acquisition-mission/SpecialistInputs.js` | Per-specialist input mapping |
| `packages/acquisition-mission/Mission.js` | Seeds draft + plan approval gate |
| `packages/max/workspace/AmoOperatorApproval.js` | Plan approval + structured Scout delegation |
| `public/acquisition-missions.html` | Mission Understanding panel |

## Tests

- `packages/acquisition-mission/tests/spec130.test.js`
- Updated SPEC-128 approval tests for two-phase flow

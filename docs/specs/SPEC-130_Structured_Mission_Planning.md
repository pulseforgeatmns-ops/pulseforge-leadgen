# SPEC-130 — Mission Planning Engine (MPE)

**Status:** Implemented  
**Depends on:** SPEC-118 (Acquisition Mission Orchestration), SPEC-128 (Operator Approval)  
**ADR:** [ADR-056](../adr/ADR-056_Mission_Planning_Engine_Is_The_Single_Interpreter.md)

## Purpose

Introduce a dedicated Mission Planning Engine that converts operator intent into a structured, governed Mission Plan **before** any specialist executes.

The Mission Plan is the canonical contract for the lifetime of the mission.

## Design Principle

**Interpret once. Execute many.**

Today every specialist interprets operator intent. Tomorrow only one component does.

## Architecture

```
Operator
      │
Natural Language
      │
Mission Planning Engine
      │
Mission Plan
      │
Mission Approval
      │
Scout · Paige · Vera · Rex · Future Specialists
```

No specialist parses operator English.

## Responsibilities

Mission Planning owns:

- intent extraction
- entity extraction
- structured planning
- ambiguity detection
- normalization
- validation
- operator confirmation

Mission Planning **never** performs execution.

## Pipeline

```
Operator Request
        │
Intent Analysis
        │
Entity Extraction
        │
Mission Structuring
        │
Ambiguity Detection
        │
Operator Review
        │
Mission Lock
        │
Execution
```

## Intent Analysis

Mission Planning determines mission type. Specialists must not.

Examples: Acquisition, Retention, Expansion, Marketing, Hiring, Operations, Research, Support, Knowledge.

## Entity Extraction

Immutable mission fields:

- Objective
- Market
- Buyer
- Industry
- Region
- Constraints
- Success Metric
- Priority
- Evidence Policy

Every structured field carries provenance (`value`, `confidence`, `reason`, `source`).

## Ambiguity Detection

Instead of guessing, Max asks.

Example:

> Operator: Find STR operators around Manchester.  
> Planner: Manchester NH or Manchester UK?  
> Operator chooses. Mission updated.

Example:

> Operator: Find property managers.  
> Planner: Residential? Commercial? Short-term rental? Mixed?

No guessing.

## Operator Confirmation

Before execution the operator sees Mission Understanding:

- Objective
- Market
- Region
- Success
- Constraints
- Evidence Threshold

Proceed? **Approve** · **Edit** · **Cancel**

Once approved, the Mission Plan is immutable.

## Specialist Contracts

| Specialist | Receives | Must not infer |
|---|---|---|
| Scout | segment, industry, buyer, geography, constraints, evidence_policy, success_metric | market, region, English |
| Paige | audience, campaign_goal, market, constraints, tone, objective | target audience |
| Vera | market, companies, buyer, review_policy | market |
| Rex | mission, progress, objective, KPIs | NL parsing |

## Blueprint Relationship

Blueprint is context. Mission is authority.

```
Blueprint
        │
Reference Only
        │
Mission Planner
        │
Mission Plan
        │
Specialists
```

Blueprint informs. Mission decides.

## Context Precedence

```
Operator Approval
        ↓
Mission Plan
        ↓
Workspace State
        ↓
Blueprint
        ↓
Historical Memory
        ↓
General Knowledge
```

Nothing may override the approved Mission Plan.

## Explainability

Mission Planning stores every interpretation.

Example:

> Operator wrote `acquire one recurring STR client`  
> Planner interpreted `segment: short_term_rental`  
> `confidence: 0.96`  
> `reason: "Matched STR operator taxonomy."`

## Implementation

| Module | Role |
|---|---|
| `packages/acquisition-mission/MissionPlanner.js` | Intent → extraction → structure → ambiguity → confirmation |
| `packages/acquisition-mission/StructuredMission.js` | Schema, provenance, freeze, confirmation copy |
| `packages/acquisition-mission/ContextPrecedence.js` | Precedence ranks; Blueprint cannot override operator fields |
| `packages/acquisition-mission/SpecialistInputs.js` | Per-specialist structured contracts |
| `packages/acquisition-mission/Mission.js` | Seeds draft, clarification, or plan-approval gate |
| `packages/max/workspace/AmoOperatorApproval.js` | Clarify / approve / edit / cancel; freeze; Scout only after lock |
| `public/acquisition-missions.html` | Mission Understanding + Approve / Edit / Cancel |

## Tests

- `packages/acquisition-mission/tests/spec130.test.js`
- Updated SPEC-128 approval tests for two-phase flow plus clarification

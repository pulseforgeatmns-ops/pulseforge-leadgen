# SPEC-165 — Strategic Decision Engine

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Max |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-164](SPEC-164_Opportunity_Intelligence_Engine.md) |
| **ADR** | [ADR-085](../architecture/ADR-085_Strategic_Resource_Allocation.md) |

## Problem

Opportunity Intelligence ranks which opportunities matter. Max still recommends the top opportunity as if attention were unlimited. Operators do not have unlimited attention. They have hours, AOs, and competing work. Ranking ABC first does not answer whether pursuing ABC today is the best use of the day.

## Objective

Introduce a Strategic Decision layer that allocates finite resources across opportunities and competing work to maximize the mission objective. Max explains tradeoffs — what is gained, what is delayed, expected business outcome, and confidence — not just which company is "best."

## Pipeline

```
Scout → Business Judgment → Opportunity Intelligence → Strategic Decision → Operator
```

| Layer | Question |
|---|---|
| Scout | What is true? |
| Opportunity Intelligence | What matters? |
| Strategic Decision | What should the business actually do today? |

## Core Module

`packages/max/decision/StrategicDecisionEngine.js`

Max integration: `packages/max/opportunity/OpportunityReasoning.js` (`ensureStrategicDecision`)

## Decision Model

Every daily recommendation is an **allocation**, not an activity list.

Evaluated against:

- **Capacity** — available hours × available AOs
- **Opportunities** — ranked SPEC-164 opportunities with expected value, hours required, reachability
- **Competing work** — direct mail, proposal follow-up, Scout review, other mission work
- **Opportunity cost** — what is delayed if this allocation is chosen
- **Mission objective** — expected business outcome of the mix, not inherent goodness of a channel

Activities (phone, door knocking, proposal follow-up, Scout review) are never recommended because they are inherently good. They appear only when they maximize the mission objective under capacity.

## Operator Contract

Max can say:

> You have 1 AO, 4 available hours, 12 opportunities. Here's the optimal allocation.

And:

> If we pursue ABC today…
>
> **Pros** — Highest recurring value; Strong buying signals
>
> **Cons** — Consumes 4 hours; Delays XYZ; Delays direct mail
>
> **Expected outcome** — +$2,800 ARR
>
> **Confidence** — 81%

## Invariant

Every Max daily recommendation must include:

1. Explicit resource allocation (hours × activity)
2. Tradeoffs of the recommended path (pros, cons, delayed work)
3. Expected business outcome
4. Confidence

No recommendation may be "do this activity" without stating why that allocation maximizes the mission objective.

## Acceptance Criteria

| Scenario | Behavior |
|---|---|
| Pursue ABC today | Tradeoff card: pros, cons (hours consumed, delayed XYZ, delayed competing work), expected ARR, confidence |
| 1 AO / 4 hours / 12 opportunities | Capacity statement + optimal allocation totaling available hours |
| Mixed day | Phone, door knocking, proposal follow-up, Scout review appear only when they maximize the objective |
| Mission Intelligence Report | Includes today's allocation, tradeoffs, expected business outcome, confidence |
| Constraint change | Fewer hours or more AOs recalculates the allocation |
| All-in vs mix | Engine compares concentrating on the top opportunity versus a mixed day and picks the higher expected mission outcome |
| Why this mix? | Explanation cites mission objective and opportunity cost — not "phone is a good activity" |

## Tests

`test/maxStrategicDecisionEngine.test.js`

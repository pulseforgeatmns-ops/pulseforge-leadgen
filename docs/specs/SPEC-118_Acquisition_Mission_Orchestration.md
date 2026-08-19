# SPEC-118 — Acquisition Mission Orchestration

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Max |
| **Created** | 2026-08-19 |
| **Depends on** | [ADR-010](../adr/ADR-010_Mission_Engine.md), [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-094](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md), [SPEC-117](SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md), [ADR-003](../adr/ADR-003_Human_Approval.md), [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) |
| **ADR** | [ADR-055 Max Manages Missions, Not Agents](../adr/ADR-055_Max_Manages_Missions.md) |

## Objective

Transform acquisition from a series of independent AI interactions into a durable, inspectable mission executed collaboratively by Pulseforge.

A mission is no longer "send emails."
A mission is a living object that every capability contributes to.

## Philosophy

```text
Max doesn't manage agents.
Max manages missions.
Agents (capabilities) contribute evidence and execution toward the mission.
```

## Vision References

- [ADR-055 Max Manages Missions, Not Agents](../adr/ADR-055_Max_Manages_Missions.md)
- [ADR-010 Mission Engine](../adr/ADR-010_Mission_Engine.md)
- [ADR-003 Human Approval](../adr/ADR-003_Human_Approval.md)
- [ADR-016 Execution Does Not Decide](../adr/ADR-016_Execution_Does_Not_Decide.md)
- [ADR-002 Explainable AI](../adr/ADR-002_Explainable_AI.md)
- [ADR-008 Outcome Intelligence](../adr/ADR-008_Outcome_Intelligence.md)
- [SPEC-022 Mission Engine](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-100 Max ↔ Scout](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md)
- [SPEC-094 Max → Paige](SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md)
- [SPEC-117 Emmett Outbound](SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md)

## Problem

Outreach today is a stack of disconnected records and chats: Scout writes prospects, Max ranks in a briefing, Paige drafts in pending comments, Emmett sends from a queue. Nothing is the mission. Operators ask "how is outreach?" and get a channel report. Failures hide in agent logs. Learning evaporates when the conversation ends.

| Today | Required |
|---|---|
| Independent agent runs | One durable Acquisition Mission |
| Outputs live in chat / agent_log | Outputs attach to the Mission |
| Silent waits | Explicit blockers |
| "Campaign succeeded" | Segment learning with a recommendation |
| Max coordinates agents | Max orchestrates mission stages |
| Specialists bleed roles | Strict capability contracts |

## Relationship to SPEC-022

SPEC-022 remains the generic capability orchestrator (`Requested → Planned → Executing → Waiting → Completed`). SPEC-118 is the **acquisition** object those capabilities contribute to.

| SPEC-022 | SPEC-118 |
|---|---|
| Intent → capability plan | Living acquisition object |
| Stage keywords / IR | Discover → … → Improve |
| Optional `orchestrationMissionId` link | Canonical campaign memory |

Do not collapse the two stores. An outbound campaign is always a SPEC-118 Mission. A SPEC-022 mission may bind to one.

## Scope (v1 thin slice)

1. Durable Acquisition Mission object (objective, segment, campaign, priority, stage, confidence, owner, created by Max)
2. Lifecycle: Discover → Understand → Plan → Prepare → Ready → Execute → Observe → Learn → Improve
3. Mission Workspace — progress, specialist states, nothing hidden
4. Capability contracts for Scout, Max, Paige, Emmett
5. Shared mission context given to every capability
6. Chronological mission timeline
7. Mission Health (not "how is outreach?")
8. Explicit blocking states — no silent failures
9. Durable learning by segment for future missions
10. Explainability from stored evidence ("Why is this mission here?")
11. Cross-capability memory (observations attach to the mission)
12. Max orchestrates stage progression; specialists cannot skip contracts
13. Operator inspect APIs + `/acquisition-missions` workspace
14. Competency `acquisition_mission_orchestration`

## Out of Scope

- Replacing SPEC-022 Mission Engine or Command Deck Operations queue
- Autonomous send (ADR-003 still gates Execute)
- Multi-channel coordination beyond email attach points
- Auto-applying learning into a live mission
- LLM-invented health, confidence, or "why" answers
- Cross-tenant mission sharing

## Mission Lifecycle

```text
Discover
    ↓
Understand
    ↓
Plan
    ↓
Prepare
    ↓
Ready
    ↓
Execute
    ↓
Observe
    ↓
Learn
    ↓
Improve
```

Every acquisition mission progresses through these stages. Max advances a stage when evidence is present. Capabilities contribute; they do not jump the lifecycle.

## Mission Object

Rather than storing dozens of disconnected records, Max creates a durable Acquisition Mission.

```text
Mission
---------
id:
mission_481

Objective:
Acquire commercial cleaning customers
in Manchester.

Target Segment:
Law Firms

Campaign:
Fall Outreach

Priority:
High

Status:
Preparing

Confidence:
0.82

Owner:
Operator

Created By:
Max
```

Everything references this mission.

## Mission Workspace

Every mission has its own workspace. Nothing is hidden.

```text
Mission

Commercial Law Firms

──────────────────────

Status

Preparing

Progress

██████░░░░

68%

Scout

✓ Discovery Complete

Max

✓ Prioritization Complete

Paige

Generating Variants

Emmett

Waiting

Operator

Approval Required
```

## Capability Contracts

| Capability | Produces | Never |
|---|---|---|
| **Scout** | Companies, Prospects, Buying signals, Decision makers, Confidence, Evidence | Messaging |
| **Max** | Priorities, Objectives, Timing, Recommendations, Constraints, Delegation | Outbound copy |
| **Paige** | Messaging, Experiments, Variants, Subjects, CTA, Learning hypotheses | Who receives it |
| **Emmett** | Capacity, Queue, Send recommendations, Deliverability intelligence, Reputation protection | Campaign messaging |

```text
Scout discovers.
Max decides.
Paige communicates.
Emmett protects and executes.

Scout never creates messaging.
Max never writes outbound copy.
Paige never decides who receives it.
Emmett never changes campaign messaging.
```

Contract violations fail closed. The contribution is rejected and recorded as a timeline event.

## Shared Mission Context

Every capability receives the same mission context.

Instead of:

> Generate email.

Paige receives:

```text
Mission
Commercial Law Firms

Objective
Generate walkthroughs.

Constraints
Operator voice
Commercial only
Veteran discount available

Campaign
Fall Outreach

Buying signals
Scout evidence

Priority reasoning
Max evidence
```

The same mission follows every capability.

## Mission Timeline

Every meaningful event becomes part of the mission. Nothing disappears into chat history.

```text
9:02   Mission Created
9:05   Scout completed discovery
9:08   Max ranked prospects
9:14   Paige generated Variant B
9:17   Operator edited CTA
9:19   Emmett approved capacity
9:20   18 emails queued
9:26   Campaign launched
10:04  First open
11:42  Reply received
2:10   Walkthrough booked
```

## Mission Health

Instead of asking "How is outreach?" Max answers from the mission:

```text
Mission Health

Healthy

Confidence
0.87

Current Blocker
Operator approval

Risk
Low

Capacity
18 remaining

Replies
3

Meetings
1

Learning
Commercial firms responding
better to operational messaging.
```

## Blocking States

Every mission explicitly exposes why it cannot proceed. No silent failures.

| Blocker | Meaning |
|---|---|
| Waiting for Operator | Approval or edit required |
| Waiting for Paige | Variants / CTA not ready |
| Waiting for Emmett | Capacity or queue not ready |
| Waiting for Scout | Discovery incomplete |
| Waiting for Max | Prioritization incomplete |
| Waiting for Domain Warm-up | Emmett warmup not healthy |
| Waiting for More Prospects | Qualified set below the mission threshold |
| Paused — Deliverability Risk | Governor Pause / Emergency |

## Learning

Mission outcomes become durable evidence. Not "Campaign succeeded."

```text
Mission Learning

Law firms
Reply Rate
14%

Property managers
Reply Rate
6%

Medical
Reply Rate
4%

Recommendation
Increase law firm allocation.
```

Learnings never auto-mutate a live mission. Max may recommend; the operator (or a later Improve stage) applies.

## Explainability

At any time the operator can ask: **Why is this mission here?**

Max answers from mission evidence. Not generated opinion.

```text
Mission exists because

Commercial revenue remains
primary objective.

Scout identified 61 qualified firms.

Inbox capacity available.

Previous campaign
produced 11% reply rate.

Confidence
0.84
```

Missing evidence is omitted, never invented.

## Cross-Capability Memory

Every capability contributes observations. The mission is shared memory.

```text
Scout   Company hired Operations Manager.
Paige   Variant C generated highest replies.
Emmett  Tuesday mornings improve deliverability.
Max     Recommend increasing campaign volume.
```

## Architecture

```text
Max creates Acquisition Mission
        ↓
Shared context envelope
        ↓
Scout / Max / Paige / Emmett / Operator
  contribute under contracts
        ↓
Timeline · Workspace · Health · Blockers
        ↓
Max advances lifecycle when evidence is ready
        ↓
Execute (operator-approved) → Observe → Learn
        ↓
Learning store (tenant-scoped, never auto-applied)
```

v1 reasoning is deterministic. No LLM invents health, confidence, blockers, or "why."

## Data Model

Tables: `acquisition_missions`, `acquisition_mission_events`, `acquisition_mission_contributions`, `acquisition_mission_observations`, `acquisition_mission_outcomes`, `acquisition_mission_learning`.

Tenant isolation: `tenant_id` / `client_id`. Cross-tenant reads fail closed.

Stages: `discover` \| `understand` \| `plan` \| `prepare` \| `ready` \| `execute` \| `observe` \| `learn` \| `improve`.

## Implementation Plan

1. Spec + ADR-055 + competency
2. `packages/acquisition-mission` deterministic engine
3. Persistence + APIs + `/acquisition-missions` workspace
4. Max Ask answers "why is this mission here?" and "how is outreach?" from evidence
5. Scout / Paige / Emmett attach contributions when a `missionId` is present
6. Tests

## Migration Strategy

Additive. `migrations/2026-08-19-acquisition-mission-orchestration.sql` plus rollback. Schema is also ensured on first service use. Existing tenants start with no missions.

## Testing

- `packages/acquisition-mission/tests/amo.test.js` — object, lifecycle, contracts, workspace 68%, timeline, health, blockers, learning, explainability, shared context, memory, tenant isolation
- `test/acquisitionMission.test.js` — service, routes, competency, Max Ask, specialist attach points

## Acceptance Criteria

- [x] Every outbound campaign is represented as a durable Mission
- [x] All capability outputs attach to the Mission instead of existing only in conversation
- [x] The operator can inspect progress, blockers, evidence, and outcomes at any stage
- [x] Every transition is recorded in a chronological mission timeline
- [x] Mission outcomes become reusable intelligence for future campaigns
- [x] Max orchestrates mission progression while preserving strict specialist boundaries

## Future Work

- Bind SPEC-022 orchestration missions automatically to SPEC-118 objects
- Multi-channel (LinkedIn, SMS) contributions under the same mission
- Command Deck mission card sourced from SPEC-118 health
- Operator in-place CTA editing as a first-class contribution UI
- Auto-suggest Improve allocations without applying them

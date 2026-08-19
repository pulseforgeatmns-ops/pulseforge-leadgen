# ADR-055 — Max Manages Missions, Not Agents

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-19 |
| **Spec** | [SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md) |
| **Related** | [ADR-010](ADR-010_Mission_Engine.md), [ADR-011](ADR-011_Capability_Framework.md), [ADR-003](ADR-003_Human_Approval.md), [ADR-016](ADR-016_Execution_Does_Not_Decide.md), [ADR-002](ADR-002_Explainable_AI.md), [ADR-054](ADR-054_Reputation_Is_Capital.md) |

## Context

Pulseforge already has a Mission Engine ([ADR-010](ADR-010_Mission_Engine.md) / [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md)) that turns operator intent into a capability plan. Specialists are also real: Scout discovers ([SPEC-100](../specs/SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md)), Paige communicates ([SPEC-094](../specs/SPEC-094_Max_to_Paige_Campaign_Content_Delegation.md)), Emmett protects reputation ([SPEC-117](../specs/SPEC-117_Emmett_Outbound_Infrastructure_Intelligence.md) / [ADR-054](ADR-054_Reputation_Is_Capital.md)).

Those pieces still run as independent interactions. An outbound campaign is not a durable object. Capability output lives in chat, `agent_log`, or a send plan. Operators cannot inspect why a mission exists, what is blocking it, or what the last campaign taught. Max is asked to "manage the agents."

Acquisition needs a living object that every capability contributes to — without collapsing specialist contracts or auto-sending.

## Decision

**Max doesn't manage agents. Max manages missions. Agents (capabilities) contribute evidence and execution toward the mission.**

1. **Every outbound campaign is an Acquisition Mission** ([SPEC-118](../specs/SPEC-118_Acquisition_Mission_Orchestration.md)). It is durable, tenant-scoped, and inspectable.
2. **Max orchestrates lifecycle**, not personalities. Stages are Discover → Understand → Plan → Prepare → Ready → Execute → Observe → Learn → Improve. Max advances a stage from evidence. Specialists do not skip stages or rewrite the mission.
3. **Capability contracts are fail-closed.** Scout never writes messaging. Max never writes outbound copy. Paige never decides who receives it. Emmett never changes campaign messaging.
4. **Shared context follows the mission.** Capabilities receive the same objective, constraints, campaign, buying signals, and priority reasoning — not a bare "generate email."
5. **Nothing is silent.** Progress, blockers, timeline events, and health are first-class. "How is outreach?" is answered as Mission Health from stored evidence.
6. **Explainability is evidence, not opinion.** "Why is this mission here?" is assembled from attached evidence. Missing facts are omitted.
7. **Outcomes become learning, not auto-strategy.** Segment reply rates and allocation recommendations persist for future missions. They do not mutate a live campaign.
8. **SPEC-022 remains the generic orchestrator.** SPEC-118 is the acquisition object. An optional `orchestrationMissionId` may bind them. Do not merge the stores.
9. **Human approval still gates Execute** ([ADR-003](ADR-003_Human_Approval.md)). Ready is not send.

## Consequences

### Positive

- Operators inspect one living mission instead of stitching agent logs
- Specialist boundaries stay intact while work is shared
- Future campaigns inherit evidence, not folklore
- Blockers are explicit; silent waits are a bug

### Negative / tradeoffs

- Two mission concepts until SPEC-022 cards read SPEC-118 health
- Specialists must pass `missionId` to attach; unattached legacy runs remain possible until callers are fully wired
- v1 health/confidence/progress are deterministic heuristics, not a learned model

### Follow-ups

- [x] SPEC-118 Acquisition Mission Orchestration (v1 thin slice)
- [ ] Command Deck Operations cards sourced from SPEC-118 health
- [ ] Automatic bind from SPEC-022 campaign-creation missions
- [ ] Multi-channel contributions under one mission

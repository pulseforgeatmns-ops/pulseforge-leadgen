# ADR-011 — Capability Framework

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-07-27 |
| **Spec** | [SPEC-023](../specs/SPEC-023_Capability_Framework.md) |
| **Supersedes** | — |
| **Related** | [ADR-010](ADR-010_Mission_Engine.md) |

## Context

[ADR-010](ADR-010_Mission_Engine.md) establishes the Mission Engine and states that missions invoke work through a Capability Registry, not agent names. [SPEC-022](../specs/SPEC-022_Mission_Engine_and_Agent_Orchestration.md) describes planner/executor and Mission-First UX, but does not freeze the executable contract.

Without a locked capability API, MissionExecutor will accrete agent-specific branches, planners will hardcode routing, and replacing Scout (or any agent) will force changes into Max and the mission layer.

## Decision

1. **Capabilities are the stable API of Pulseforge. Agents are implementation details.**
2. Every executable action exposed to missions implements the [SPEC-023](../specs/SPEC-023_Capability_Framework.md) `Capability` contract (`canRun`, `estimate`, `execute`, optional `rollback`, JSON schemas, category, failure metadata).
3. **`CapabilityRegistry`** is the only discovery and resolution surface for `MissionPlanner` and `MissionExecutor`. Planners never import concrete implementations; executors never branch on agent module names.
4. Capabilities receive **`CapabilityContext` only** — no ambient global state queries inside capability implementations.
5. Capabilities return structured **`CapabilityResult`** (outputs, evidence, artifacts, warnings, errors, nextRecommendations). Results are durable and feed Knowledge.
6. Progress is emitted as typed events (Queued / Running / Progress / Completed / Failed / Retrying / Cancelled) for Mission Workspace.
7. v1 ships five built-ins: Prospect Discovery, Company Enrichment, Knowledge Update, Opportunity Ranking, Campaign Builder. Additional catalog entries register later without redesigning Mission Engine.

## Consequences

### Positive

- Scout (or any agent) can be replaced without changing missions, planner, or operator-facing copy
- Uniform observability, retry, and replay semantics
- Capability tests stay independent of full mission orchestration
- Clear boundary: agents behind adapters; Max only sees capabilities

### Negative / tradeoffs

- Adapter layer required for every legacy agent before it is mission-callable
- Schema discipline (input/output JsonSchema) adds upfront work vs ad-hoc function calls
- Parallel paths until all operator work moves onto capabilities (`/api/run/:agent` remains per ADR-010)

### Follow-ups

- [x] Implement SPEC-023 registry + runner + five built-in stub adapters
- [x] Wire SPEC-022 MissionPlanner / MissionExecutor exclusively through the registry
- [x] Architecture test: no agent filename imports in planner/executor
- [x] Replace Prospect Discovery stub with live capability ([SPEC-024](../specs/SPEC-024_Prospect_Discovery_Capability.md))
- [x] Replace Opportunity Ranking stub with live capability ([SPEC-026](../specs/SPEC-026_Opportunity_Ranking_Capability.md))
- [x] Proposal Generator as Mission capability ([SPEC-027B](../specs/SPEC-027B_Proposal_Generator_Capability.md) / [ADR-014](ADR-014_Personalized_by_Default.md))
- [x] Client Playbook strategy assets ([SPEC-028](../specs/SPEC-028_Client_Playbook_Capability.md) / [ADR-015](ADR-015_Strategy_Lives_in_the_Playbook.md))
- [ ] Replace enrichment / knowledge / campaign stubs with live adapters
- [ ] Execution Engine capability ([SPEC-029](../specs/SPEC-029_Execution_Engine.md) / [ADR-016](ADR-016_Execution_Does_Not_Decide.md))
- Update CURRENT_STATE when remaining live adapters ship

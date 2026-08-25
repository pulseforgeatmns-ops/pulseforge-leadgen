# SPEC-181 — Evidence-Native Execution

| Field | Value |
|---|---|
| **Status** | Draft |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Epic** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md) |
| **Depends on** | [SPEC-180](SPEC-180_Single_Investigation_Planner.md), [SPEC-177](SPEC-177_Hypothesis_Driven_Discovery_Engine.md), [SPEC-172](../packages/acquisition-mission/tests/spec172.test.js) |
| **Supersedes** | Provider-first execution in leadgen.js, executeCoveragePlan as primary path |

## Objective

Scout executes investigations by collecting evidence types — not by calling providers directly. The execution bridge connects the investigation plan to provider adapters through evidence requirements. Production cron Scout uses the same bridge as Mission Engine discovery.

## Problem

Execution is fragmented:

| Path | Execution Model | Production? |
|---|---|---|
| `HypothesisDrivenDiscoveryEngine` | Evidence task → provider assignment → collection | Mission path (default) |
| `executeCoveragePlan` (SPEC-153) | City × Concept × Source grid | Legacy fallback |
| `leadgen.js` | Direct SerpAPI + Places loops | **Cron Scout (production)** |
| `scoutPublicSourcing.js` | Direct Places Text Search | Public-source work requests |

Provider deletion currently changes reasoning on the cron path because providers *are* the reasoning.

## Decision

Evidence-native execution follows:

```
InvestigationPlan.tasks[]
  ↓
EvidenceProviderAssignment (SPEC-182)
  ↓
ProviderEvidenceContract (normalize reports)
  ↓
EvidenceFusion + Identity Resolution
  ↓
InvestigationState update
```

## Execution Bridge

| Module | Role |
|---|---|
| `coverage/HypothesisDrivenDiscoveryEngine.js` | Canonical execution orchestrator |
| `coverage/EvidenceProviderAssignment.js` | Evidence type → provider mapping |
| `coverage/ProviderEvidenceContract.js` | Normalize provider reports |
| `investigation/EvidenceExecutor.js` | Execute evidence collection tasks |
| `adapters/ScoutDiscoveryArtifact.js` | AMO handoff boundary (SPEC-172) |

## Cron Scout Migration (Phase 3)

`leadgen.js` shall:

1. Build `MarketDefinition` from client config (`client_id`, industry, location).
2. Call `runHypothesisDrivenDiscovery()` (or thin wrapper around `Scout.discover()`).
3. Persist candidates through existing DB insert path (operational adapter).
4. Remove inline SerpAPI/Places search loops.

## Invariants

1. No entry point calls `adapter.discover()` without an investigation task ancestor.
2. Provider failures trigger plan revision (`revisePlanForUnavailableProviders`), not reasoning changes.
3. `executeCoveragePlan` remains available only when no `MarketDefinition` exists (pre-cognitive legacy).
4. Execution bridge output feeds `ScoutDiscoveryArtifact` for AMO missions.

## Acceptance Criteria

- [ ] Mission-path discovery uses evidence-native execution exclusively
- [ ] `CandidateUniverse` does not call `executeCoveragePlan` when `marketDefinition` is present
- [ ] Provider failure produces plan revision, not alternate reasoning path
- [ ] `leadgen.js` migration tracked as Phase 3 (separate PR)

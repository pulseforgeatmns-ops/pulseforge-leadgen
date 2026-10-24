# SPEC-161 — Market Memory

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Priority** | Critical |
| **Owner** | Scout |
| **Created** | 2026-08-25 |
| **Depends on** | [SPEC-143](SPEC-143_Scout_Intelligence_Memory.md), [SPEC-158](SPEC-158_Market_Definition_Hypothesis_Engine.md), [SPEC-159](SPEC-159_Investigative_Reasoning_Loop.md), [SPEC-160](SPEC-160_Evidence_Synthesis_Engine.md) |
| **ADR** | [ADR-081](../architecture/ADR-081_Markets_Are_Living_Systems.md) |

## Problem

Every mission begins largely as a new investigation. Scout remembers individual observations and mission artifacts, but it does not maintain a continuously evolving understanding of a market. Previous discoveries are rediscovered, market evolution is not measured, confidence resets unnecessarily, and investigation effort is duplicated.

## Objective

Introduce **Market Memory** — synthesized understanding that persists across missions and continuously measures how markets evolve over time.

Scout begins every investigation by asking: **What do I already know, and what has changed?**

## Architectural Principle

Replace:

```
Mission → Investigate → Report → End
```

With:

```
Mission → Recall Market Memory → Investigate Changes → Update Understanding → Remember → Future Mission
```

## Market Memory Model

```javascript
{
  marketId, geography, industries, entities, relationships,
  marketUnderstanding, historicalSnapshots, confidence, lastUpdated
}
```

Unlike CRM records, Market Memory stores **understanding**, not contacts.

## Business Memory

Each business becomes an evolving understanding:

```javascript
{
  entityId, currentUnderstanding, historicalUnderstandings,
  confidenceHistory, buyingSignalHistory, relationshipHistory, evidenceTimeline
}
```

Understanding is never overwritten — prior understanding is archived with a revision reason.

## Change Detection

Scout compares previous understanding → current evidence → difference. Every difference becomes a first-class observation.

## Mission Intelligence Report

New section: **Market Changes Since Last Investigation**

- New operators
- Removed operators
- Businesses expanded
- Buying signals increased
- Confidence change
- Outstanding unknowns

## New Invariant

Every investigation begins by recalling existing market understanding before collecting new evidence. Scout investigates **changes**, not just markets.

## Implementation

| Module | Role |
|---|---|
| `packages/scout/memory/MarketMemory.js` | Business memory, change detection, snapshots, merge |
| `packages/scout/DiscoveryPipeline.js` | Load memory at start, persist at end |
| `packages/scout/memory/index.js` | `persistDiscoveryKnowledge()` |
| `packages/scout/investigation/MissionIntelligenceReport.js` | Market changes section (via pipeline) |

## Acceptance Criteria

| Scenario | Expected |
|---|---|
| Scout revisits a previously investigated market | Existing Market Memory loaded |
| Known business changes | Difference detected; historical timeline updated |
| No meaningful changes | Confidence increases; duplicate investigation avoided |
| Contradictory evidence | Understanding revised; prior retained; reason recorded |
| Mission Intelligence Report | Displays market changes since last investigation |

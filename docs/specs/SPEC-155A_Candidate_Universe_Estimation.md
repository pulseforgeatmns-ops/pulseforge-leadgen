# SPEC-155 — Candidate Universe Estimation

> **Status:** Implemented  
> **Priority:** Critical  
> **Owner:** Scout  
> **ADR:** [ADR-078 — Estimate Before Investigation](../architecture/ADR-078_Estimate_Before_Investigation.md)

## Problem

Scout began searching without first estimating what it expected to find. Coverage lacked a denominator, confidence was difficult to interpret, and "0 candidates" provided little context.

## Objective

Before executing any investigation, Scout estimates the expected size of the target market. This estimate becomes the baseline for all subsequent discovery decisions.

## Architectural Principle

```
Mission → Estimate Market → Build Investigation Plan → Search → Compare Reality Against Estimate
```

## Universe Estimate

```typescript
interface CandidateUniverseEstimate {
  minimum: number;
  expected: number;
  maximum: number;
  confidence: number;
  reasoning: string[];
  revisionHistory?: object[];
}
```

## Implementation

| Module | Role |
|---|---|
| `packages/scout/universe/CandidateUniverseEstimate.js` | Multi-signal estimation, revision, coverage invariant |
| `packages/scout/DiscoveryPipeline.js` | Stage 2 `ESTIMATE_UNIVERSE` before execution |
| `packages/scout/intelligence/MarketCoverage.js` | Coverage relative to explicit estimate only |
| `packages/scout/coverage/DiscoveryCoverageEngine.js` | Discovery report includes estimate + market coverage |
| `packages/scout/intelligence/IntelligenceReport.js` | Mission Intelligence Report fields |

## Estimation Signals

- Coverage plan geometry (searches × cities × concepts)
- CRM / existing intelligence counts
- Geographic footprint
- Historical mission memory (`marketSize`)
- Industry density ratios by segment

Missing signals reduce **estimate confidence**, not estimation itself.

## Invariants

1. Every investigation begins with an estimated candidate universe.
2. Coverage percentages may only be computed relative to an explicit estimate.
3. Estimate revisions require reasoning and are recorded in `revisionHistory`.

## Acceptance Criteria

- [x] Scenario 1: Universe estimate generated before external discovery
- [x] Scenario 2: Estimate produced without external sources; confidence reduced
- [x] Scenario 3: Material evidence triggers revision with explanation
- [x] Scenario 4: Mission Intelligence Report displays min/expected/max/confidence/coverage
- [x] Scenario 5: Coverage cannot be reported without an estimated universe

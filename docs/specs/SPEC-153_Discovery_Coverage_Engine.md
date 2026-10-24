# SPEC-153 — Discovery Coverage Engine

**Status:** Implemented  
**Priority:** High  
**Owner:** Scout  
**Supersedes:** Current single-query discovery strategy  
**ADR:** ADR-076 — Coverage Before Conclusion

## Problem

Discovery previously treated a mission as a single search query (e.g. `"short term rental Greater Manchester"`), causing ambiguous geography, incomplete market coverage, zero candidate universes, misleading confidence, and false "market empty" conclusions.

## Objective

Scout executes a structured investigation plan that measures and maximizes market coverage before concluding that a candidate universe is empty. **Discovery owns coverage, not search.**

## Architecture

```
Mission → Discovery Strategy → Coverage Plan → Source Execution → Candidate Universe → Qualification → Prioritization
```

### Key modules

| Module | Path | Role |
|---|---|---|
| Concept expansion | `packages/scout/coverage/ConceptLibrary.js` | Expands mission segments into searchable concept variants |
| Coverage engine | `packages/scout/coverage/DiscoveryCoverageEngine.js` | Builds plan, executes City×Concept×Source workloads, tracks coverage |
| Universe construction | `packages/max/scoutAcquisition/CandidateUniverse.js` | Seeds from existing intelligence, runs coverage plan |
| Scout adapter | `packages/max/scoutAcquisition/ScoutAdapter.js` | Attaches coverage report, discovery status, confidence |
| AMO payload | `packages/acquisition-mission/DiscoveryPayload.js` | Normalizes coverage for operator review |
| UI | `public/acquisition-missions.html` | Mission Intelligence Report coverage panel |

## Invariants

1. **Discovery Coverage** — A discovery mission never concludes before executing its Discovery Plan.
2. **Existing Intelligence** — Known qualifying candidates are included in the Candidate Universe before external discovery.
3. **Empty Universe** — "No candidate universe" is only valid after the Discovery Plan reaches completion.
4. **Confidence** — Discovery confidence measures investigation completeness, not market existence.

## Acceptance Criteria

| Scenario | Expected |
|---|---|
| Greater Manchester geography | 6 cities searched |
| Short-term rental operators | All configured STR terminology executed |
| CRM contains qualifying operators | Candidate Universe non-empty before external discovery |
| Google Places returns zero | Alternate sources execute automatically |
| Coverage incomplete | Discovery Status = Incomplete; no prioritization |
| Coverage complete, zero qualified | Candidate Universe = 0; confidence reflects complete investigation |

## Related specs

- SPEC-100A — Scout Acquisition Discovery Foundation
- SPEC-141 — Scout Intelligence Pipeline (Market Coverage stage)
- SPEC-133 — Discovery Artifact Presentation

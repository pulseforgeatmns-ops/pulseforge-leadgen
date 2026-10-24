# ADR-079 — Understanding Before Recommendation

**Status:** Accepted  
**Date:** 2026-08-25  
**Spec:** SPEC-159 Investigative Reasoning Loop

## Context

Scout previously risked treating search results as conclusions. A coverage pass that returned 27 "Vacation Property Management" listings could immediately produce outreach recommendations without updating the semantic model of the market.

This is the difference between a search system and an intelligence system.

## Decision

Scout **must not** recommend actions directly from evidence.

Scout **must**:

1. Fuse new evidence into `InvestigationState`
2. Revise market definition, universe estimate, and hypotheses when understanding changes
3. Evolve confidence with recorded reasons
4. Generate recommendations only from the updated understanding

## Consequences

### Positive

- Operators see why confidence changed (confidence evolution trail)
- Failed hypotheses are archived with reasons, not discarded
- Repeat investigations seed from prior understanding (SPEC-143 memory)
- Mission Intelligence Reports explain remaining unknowns

### Negative

- Additional processing after coverage execution
- Pipeline result now carries `investigationState` and `missionIntelligenceReport`

## Enforcement

- `InvestigativeReasoningLoop.runInvestigativeReasoningLoop()` sets `understandingFirst: true`
- `buildRecommendationFromUnderstanding()` sets `basedOnUnderstanding: true` and `notDirectFromEvidence: true`
- `DiscoveryPipeline` runs the reasoning loop by default (`useInvestigativeReasoningLoop !== false`)

## Related

- SPEC-142 — Evidence-Driven Investigation Engine (entity-level loop, tests/internal)
- SPEC-145 — Adaptive Investigation Planning
- SPEC-158 — Market Definition & Hypothesis Engine
- SPEC-143 — Scout Intelligence Memory (prior understanding seeding)

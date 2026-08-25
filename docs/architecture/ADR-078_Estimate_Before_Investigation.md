# ADR-078 — Estimate Before Investigation

## Status

Accepted

## Context

Scout investigations produced candidate counts without a prior market-size expectation. Operators could not interpret coverage, sparse results, or zero-result outcomes.

## Decision

An investigation is only meaningful relative to an expected market. Scout must estimate the candidate universe **before** external discovery and compare discovered reality against that estimate.

## Consequences

- `DiscoveryPipeline` stage 2 (`ESTIMATE_UNIVERSE`) is mandatory and precedes provider execution.
- `CandidateUniverseEstimate` combines multiple evidence signals; missing signals reduce confidence, not estimation.
- `computeCoverageFromEstimate()` returns `null` when no estimate exists — coverage percentages are forbidden without a denominator.
- Estimates may be revised during investigation; every revision records reasoning in `revisionHistory`.
- Mission Intelligence Reports surface `estimatedMarket`, `investigated`, and `marketCoveragePct`.

## References

- SPEC-155A — Candidate Universe Estimation
- SPEC-154 — Unified Discovery Pipeline
- SPEC-153 — Discovery Coverage Engine

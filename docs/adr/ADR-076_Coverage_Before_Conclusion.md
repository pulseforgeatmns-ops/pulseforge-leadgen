# ADR-076 — Coverage Before Conclusion

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Spec** | [SPEC-153](../specs/SPEC-153_Discovery_Coverage_Engine.md) |
| **Audit** | [AUDIT-044](../architecture/AUDIT-044_Discovery_Coverage_Investigative_Reasoning.md) |
| **Related** | [ADR-045](ADR-045_Evidence_Before_Reasoning.md), [ADR-049](ADR-049_Understand_Market_Before_Selling.md), [ADR-040](ADR-040_Separate_Evidence_Acquisition_from_Capability_Selection.md) |

## Context

Scout previously treated discovery as a single search query against mission wording. A zero-result query was interpreted as an empty market. Operators could not distinguish "we investigated thoroughly and found nothing" from "we searched once and gave up."

PulseForge missions require evidence-based business decisions from Max. An empty candidate universe is a **conclusion**, not a search outcome. Conclusions require measured investigation coverage.

## Decision

**Discovery is investigation, not search.**

1. **Scout shall not conclude that a market is empty** until its Discovery Coverage Plan reaches completion across geography, terminology, and configured sources.
2. **Discovery confidence measures investigation completeness**, not market existence or prospect quality.
3. **Existing tenant intelligence seeds the candidate universe** before external discovery runs.
4. **One source returning zero results does not terminate investigation** — remaining workloads in the coverage plan must execute.
5. **Incomplete coverage blocks prioritization and empty-universe conclusions** — `discoveryStatus = incomplete` until the plan is addressed.
6. **Stopping requires an explicit rationale** — coverage threshold met, branches exhausted, budget/API limits, or manual investigation required. "Zero prospects" alone is never sufficient.

## Architectural principle

> Discovery is the process of reducing uncertainty about a market until Max can make evidence-based business decisions.

Scout proves the market was investigated. Qualification and prioritization prove which candidates matter.

## Implementation

| Component | Role |
|---|---|
| `packages/scout/coverage/DiscoveryCoverageEngine.js` | City × Concept × Source plan, execution, metrics |
| `packages/scout/coverage/ConceptLibrary.js` | Terminology expansion |
| `packages/max/scoutAcquisition/CandidateUniverse.js` | Retrieve-before-discover + coverage execution |
| `packages/acquisition-mission/DiscoveryPayload.js` | Gates: `hasSufficientEvidenceForPrioritization`, empty-universe normalization |

## Consequences

### Positive

- Operators can trust "no candidates" conclusions
- Max receives coverage-backed intelligence, not search anecdotes
- Terminology and geography expansion are measurable, not implicit

### Negative / tradeoffs

- More API calls per discovery mission (bounded by plan size)
- Legacy paths (`prospect_discovery`-only, `leadgen.js`) must migrate or be explicitly excluded

### Follow-ups

- [ ] Unify `Scout.discover()` with coverage engine on all external strategies (AUDIT-044)
- [ ] Pre-search candidate universe estimation (min/expected/max/confidence)
- [ ] Platform-specific source adapters (Airbnb, VRBO, social) as optional plan workloads

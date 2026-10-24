# ADR-102 — Investigation Follows Uncertainty

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-27 |
| **Related** | AUDIT-070, SPEC-177, SPEC-194 |

## Context

Scout's production Discovery pipeline successfully produces qualified prospects and identifies unresolved questions for each prospect. For example, Lot 202 may be qualified with unknown readiness and an unresolved hypothesis such as "No identifiable operations decision-maker."

Scout also computes candidate investigation needs (`missingEvidence[]`, `unresolvedHypotheses[]`, `recommendedNextInvestigation`). However, these needs remained descriptive metadata — they did not become executable investigation tasks.

When the operator requested more Discovery evidence, Scout re-ran broad market discovery instead of continuing investigation on the businesses already discovered.

## Decision

Investigation continuation shall follow unresolved uncertainty at the entity level.

Once a canonical business identity has been established and qualified for investigation, Scout shall preserve that entity and investigate its unresolved hypotheses before repeating broad market discovery unless additional market coverage is itself the highest-value uncertainty.

### Canonical Principle

**Discovery finds the universe. Investigation reduces uncertainty within it.**

These are separate operations.

- Broad discovery answers: *Who might matter?*
- Candidate investigation answers: *What do we need to know about this specific business?*

### Investigation Unit

After identity resolution, the canonical investigation unit becomes:

**Candidate + Hypothesis + Evidence Gap**

not:

**Segment + City + Provider**

### Continuation

A request for additional evidence shall resume the existing investigation state. It shall not implicitly restart discovery.

Broad discovery may execute again only when:

- geographic coverage is insufficient,
- candidate-universe coverage is insufficient,
- a new market hypothesis requires additional businesses, or
- Scout determines that expanding the universe has higher expected information gain than deepening existing candidates.

## Implementation

- `EntityInvestigationContinuation.js` extracts unresolved entity tasks from prior discovery payloads (SPEC-194 evaluations and SPEC-144 credibility briefs).
- `DiscoveryPipeline` routes continuation requests through entity investigation when preserved candidates carry high-value unresolved questions.
- `HypothesisDrivenDiscoveryEngine` executes entity-scoped tasks (Candidate + Hypothesis + Evidence Gap) without discarding prior identities.
- `AmoOperatorApproval.advanceDiscoveryInvestigationAfterApproval` passes prior discovery state into Scout instead of triggering a blind re-discovery.

## New Invariant

A qualified prospect with a high-value unresolved question must produce an executable investigation path, not merely explanatory prose.

Continuing investigation preserves previously discovered identities and evidence.

## Consequences

- Operator "Continue investigation" deepens entity uncertainty before repeating broad market search.
- `investigationContinuation` payloads carry `investigationMode: entity_continuation | broad_discovery`.
- Entity tasks are keyed `task:{entityId}:{evidenceType}` and scoped in evidence requests.
- Prior `rankedProspects` and candidate universe records are seeded into continuation runs.

## Relationship to Prior ADRs

- **ADR-079** — Understanding before recommendation
- **ADR-083** — Investigate what reduces uncertainty most
- **ADR-095** — Single investigation planner (entity tasks extend SPEC-180 plan shape)
- **ADR-101** — Fit vs buying readiness separate (SPEC-194 supplies entity investigation inputs)

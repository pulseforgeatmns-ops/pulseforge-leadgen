# SPEC-175 — External Discovery Capability

**Status:** Implemented  
**Priority:** Critical (First Business Capability)  
**Owner:** Scout  
**Related:** SPEC-153 Discovery Coverage Engine, SPEC-141 Provider Capability Registry, AUDIT-055

## Objective

Guarantee that Scout can always produce a candidate universe for supported acquisition missions. Discovery shall fail because markets are empty, not because Scout has no search capability.

## Architectural Principle

Scout is an intelligence system. Intelligence requires observation. Observation requires sensors. A provider is a sensor. Discovery behavior depends on provider capability, not silent skips.

## Discovery Capability Model

Every external discovery provider declares one of:

| State | Meaning |
|---|---|
| `AVAILABLE` | Provider can produce candidate evidence now |
| `DEGRADED` | Provider is operational but limited |
| `UNAVAILABLE` | Provider cannot run (missing credentials, outage) |
| `STUB` | Declared but not required for candidate discovery (LinkedIn, Facebook) |
| `NOT_IMPLEMENTED` | Planned provider not yet built (Airbnb, VRBO) |

## Provider Registry

| Provider | Default Capability |
|---|---|
| Google Places | `AVAILABLE` when `GOOGLE_PLACES_KEY` or injected discover fn is present |
| LinkedIn | `STUB` |
| Facebook | `STUB` |
| Airbnb | `NOT_IMPLEMENTED` |
| VRBO | `NOT_IMPLEMENTED` |

Mission planning can immediately explain:

> I cannot investigate this market because no external discovery provider is available.

## Capability Gate

Before external discovery executes:

```
Mission → Coverage Plan → Capability Evaluation → Enough sensors? → Execute or Block
```

If no operational evidence-producing provider exists, discovery returns `DISCOVERY_BLOCKED` with reason **External Discovery Capability Unavailable**. TME preconditions fail with code `external_discovery_capability_unavailable` — discovery never enters transactional execution.

## Geography Expansion

Multi-city missions (e.g. Hooksett and Auburn) expand into independent city workloads. Each city executes all concepts before moving to the next city cluster in the coverage plan (`City × Concept × Source`).

## Candidate Minimum Contract

Before evaluation, every discovered candidate must include:

- **identity** — stable id or name
- **location** — city or address
- **source** — discovery source type
- **retrieval provenance** — provider, workload, city, concept, retrievedAt

Timing signals remain optional. Identity is not.

## Key Modules

| Module | Path |
|---|---|
| Provider registry | `packages/scout/coverage/ExternalDiscoveryProviderRegistry.js` |
| Capability gate | `packages/scout/coverage/DiscoveryCapabilityGate.js` |
| Candidate contract | `packages/scout/coverage/CandidateMinimumContract.js` |
| Pipeline integration | `packages/scout/DiscoveryPipeline.js` |
| Universe construction | `packages/max/scoutAcquisition/CandidateUniverse.js` |
| TME precondition | `packages/max/workspace/AmoOperatorApproval.js` |

## Acceptance Scenarios

| # | Scenario | Expected |
|---|---|---|
| 1 | Google Places available | Candidate universe populated; discovery continues |
| 2 | Google Places unavailable | Mission pauses before TME; blocker: External Discovery Capability Unavailable |
| 3 | Two cities (Hooksett, Auburn) | Each city executes all concepts independently |
| 4 | One provider, zero candidates | Discovery completes legitimately with complete coverage |
| 5 | No providers | Discovery never reaches qualification; capability blocked |
| 6 | End-to-end Anchor mission | Provider → Candidates → Evidence → Evaluation → Prioritization |

## Invariants

1. Scout shall not begin evidence-based external discovery without at least one operational evidence-producing provider.
2. A discovery mission blocked by missing external capability shall surface that capability gap explicitly before transactional execution.

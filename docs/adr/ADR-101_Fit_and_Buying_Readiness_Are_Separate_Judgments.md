# ADR-101 — Fit and Buying Readiness Are Separate Judgments

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-27 |
| **Spec** | SPEC-193 |
| **Related** | AUDIT-069, ADR-077 |

## Context

AUDIT-069 established that production Scout successfully queried Google Places, received businesses, established canonical identities, attached evidence and provenance, and established strong basic business fit — but produced **Qualified Prospects: 0** because `qualifyCandidate()` required a timely timing signal.

For fit candidates:

- `basicFit = true`
- `timelyTiming = false`
- `supported = false`

Those candidates were stored as `fitCandidates` but excluded from opportunities, `qualifiedCount`, `rankedProspects`, and prioritization. The absence of timing evidence was being interpreted as failure to qualify.

## Decision

Business fit and buying readiness are independent dimensions.

A prospect may be:

- **Qualified + Ready**
- **Qualified + Not Ready**
- **Qualified + Readiness Unknown**
- **Not Qualified**

The absence of a buying signal shall never by itself establish that a business is not a qualified prospect.

### Canonical Model

```
Identity
   ↓
Market / ICP Qualification
   ↓
Qualified Prospect
   ↓
Readiness Assessment
   ↓
Prioritization
```

**Qualification** asks: *Is this a business we plausibly want as a customer?*

Examples: correct business type, correct geography, appropriate scale, relevant operating model, compatible service need, no exclusionary evidence.

**Readiness** asks: *Is there evidence this business is especially worth contacting now?*

Examples: expansion, new location, hiring, vendor dissatisfaction, leadership change, portfolio growth, service complaints, contract/event timing.

These are not qualification predicates.

### Unknown Is Not Negative

Scout explicitly distinguishes:

- `NEGATIVE_EVIDENCE` — proof against readiness or fit
- `INSUFFICIENT_EVIDENCE` — unknown, not disproven

If Scout has not established whether a business outsources cleaning, that means `cleaningResponsibility = unknown`, not `cleaningResponsibility = false`. The same applies to buying readiness.

### New Invariant

Absence of a timing signal shall reduce buying-readiness confidence, not invalidate business fit.

**Qualification determines who belongs in the market. Readiness determines who deserves attention first.**

## Implementation

| Concept | Field | Values |
|---|---|---|
| Market qualification | `qualified`, `basicFit` | boolean |
| Buying readiness | `readinessState` | `ready`, `not_ready`, `unknown` |
| Evidence posture | `evidenceKind` | `positive_evidence`, `negative_evidence`, `insufficient_evidence` |
| Near-term support | `supported` | true only when readiness is `ready` with source-backed timing |

`qualifiedCount` and `rankedProspects` include all qualified prospects. Readiness affects rank order, not inclusion.

## Consequences

### Positive

- Fit candidates with unknown timing appear in operator prioritization workflows.
- Discovery no longer reports zero qualified prospects when basic fit exists.
- Rejection summaries no longer count unknown readiness as a qualification failure.

### Tradeoffs

- Prioritization approval can proceed with lower readiness confidence when only fit-qualified prospects exist.
- Operators must interpret readiness state when approving outreach order.

### Follow-ups

- Surface readiness state in Mission Workspace prospect cards.
- Tune ranking weights so `ready` prospects sort above `unknown` and `not_ready`.

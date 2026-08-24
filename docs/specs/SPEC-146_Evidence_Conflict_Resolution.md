# SPEC-146 — Evidence Conflict Resolution

**Status:** Implemented  
**Depends on:** SPEC-141, SPEC-142, SPEC-143, SPEC-144, SPEC-145  
**Related ADR:** ADR-065 — Conflicting Evidence Is Intelligence

## Objective

Introduce an Evidence Conflict Resolution Engine (ECRE) that detects, explains, and resolves conflicting evidence during Scout investigations. Conflicting evidence is never silently discarded. Every contradiction becomes a first-class intelligence object.

## Design Principle

**Disagreement is evidence, not noise.**

Scout shall never overwrite contradictory evidence. Instead Scout shall:

1. Detect conflict
2. Explain conflict
3. Attempt resolution
4. Expose remaining uncertainty

## Pipeline Integration

```
Evidence Collection
        ↓
Evidence Conflict Resolution   ← SPEC-146 (new stage)
        ↓
Qualification
```

ECRE also runs inside the SPEC-142 investigation loop after each evidence collection iteration.

## EvidenceConflict Object

```javascript
{
  id,
  subject,
  conflictingClaims: [{ source, sourceLabel, value, label, observedAt }],
  providers,
  category,       // temporal | source_authority | observation | genuine_unknown
  severity,       // low | medium | high | critical
  confidence,
  resolution: {
    strategy,     // freshness | authority | majority | context | operator_escalation
    workingEstimate,
    reason,
    resolved,
  },
  unresolvedReason,
  confidencePenalty,
}
```

## Resolution Strategies

| Strategy | When Applied |
|---|---|
| Freshness | Newer source updated within 30 days vs stale source |
| Authority | Higher-weight provider (county > website > Google > LinkedIn) |
| Majority | Three or more providers, two agree |
| Context | Hiring + small team — growth signal, not contradiction |
| Operator Escalation | No resolution possible — reduce confidence, recommend providers |

## Module Layout

| File | Purpose |
|---|---|
| `packages/scout/conflict/types.js` | EvidenceConflict builders, severity, strategies |
| `packages/scout/conflict/ConflictDetection.js` | Unified detection (SPEC-142 rules + numeric claims) |
| `packages/scout/conflict/ConflictResolution.js` | Resolution engine |
| `packages/scout/conflict/ConflictReport.js` | Mission intelligence report section |
| `packages/scout/conflict/ProviderConflictLearning.js` | Provider freshness/authority/conflict learning |
| `packages/scout/conflict/index.js` | Public API |

## Mission Intelligence Report

New section: `evidenceConflicts`

```
Evidence Conflicts: 3 detected, 2 resolved, 1 outstanding
Recommendation confidence reduced from 0.91 → 0.83
```

## Second Brain Integration

Investigation memory persists:

- `providerConflictLearning` — per-provider conflict/resolution rates
- `conflictHistory` — subject, strategy, working estimate per investigation

## Runtime Guarantees

- No conflicting evidence is silently discarded
- Every conflict is stored, explainable, traceable, and reviewable
- Unresolved conflicts generate investigation tasks with recommended providers

## Acceptance Tests

See `test/scoutEvidenceConflictResolution.test.js`:

1. Three providers disagree → conflict detected
2. Newer evidence wins → reason explained
3. Unresolved conflict → recommendation confidence reduced
4. Unresolved conflict → additional provider recommended
5. Repeated investigations → provider learning improves weighting

## Success Criteria Example

Manchester STR operator with Google Maps (15 listings), website (22), Airbnb (21), LinkedIn (hiring cleaners):

- Conflict detected across listing counts
- Google Maps flagged stale (9+ months)
- Working estimate: 21–22 listings
- Confidence: ~0.91 with explained resolution
- No information hidden; no confidence fabricated

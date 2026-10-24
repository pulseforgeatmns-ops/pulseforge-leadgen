# SPEC-144 — Scout Intelligence Credibility Framework

| Field | Value |
|---|---|
| **Status** | Implemented |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Depends on** | [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-142](SPEC-142_Evidence_Driven_Investigation_Engine.md), [SPEC-133](SPEC-133_Acquisition_Mission_Discovery_Payload.md) |

## Objective

Scout must not only produce intelligence — Scout must produce intelligence that an experienced operator would trust enough to act on.

## Philosophy

Every conclusion answers three questions:

1. **Why do you believe this?** → Evidence
2. **How confident are you?** → Reasoning
3. **What could make you wrong?** → Uncertainty

That third question is almost never present in AI systems. It becomes one of Scout's strengths.

## Intelligence Brief Structure

Every ranked opportunity receives a briefing:

```json
{
  "opportunity": { "rank": 1, "name": "Harbor Property Management" },
  "overallConfidence": 0.91,
  "confidenceExplanation": {
    "score": 0.91,
    "basedOn": [{ "label": "Website", "present": true, "freshness": "excellent" }],
    "missing": [{ "label": "Decision maker", "present": false }]
  },
  "whyRankedHere": "...",
  "evidence": [],
  "buyingSignals": [],
  "risks": [],
  "unknowns": [],
  "contradictions": [],
  "competingHypotheses": [],
  "rankingBreakdown": [],
  "trust": { "level": "medium", "reason": "..." },
  "recommendedNextInvestigation": {},
  "highestRemainingUnknowns": []
}
```

## Evidence Weighting

| Source | Weight |
|---|---|
| Official county records | 1.00 |
| Company website | 0.95 |
| Secretary of State | 0.95 |
| Google Business | 0.80 |
| LinkedIn | 0.75 |
| Facebook | 0.55 |
| Forum post | 0.20 |

## Freshness Bands

| Age | Band |
|---|---|
| ≤ 7 days | Excellent |
| ≤ 90 days | Good |
| ≤ 365 days | Low confidence |
| > 365 days or unknown | Needs verification |

## Trust vs Confidence

- **Confidence** — How sure are we?
- **Trust** — Would we act on this?

Example: High confidence (0.94) + low evidence diversity → Medium trust — needs one more independent source.

## Contradiction Detection

Scout detects numeric mismatches (e.g. website reports 15 properties, county records report 42). Instead of averaging, Scout reports:

> Contradiction detected. Confidence reduced. Recommend verification.

## Acceptance Criteria

A human operator reviewing Scout's report can answer without asking Scout another question:

- Why is this company ranked here?
- What evidence supports that ranking?
- Which evidence is strongest?
- Which evidence conflicts?
- What is still unknown?
- How risky is acting on this recommendation?
- What is the single highest-value next verification step?

## Implementation

| Module | Role |
|---|---|
| `packages/scout/credibility/EvidenceWeights.js` | Source quality weights |
| `packages/scout/credibility/EvidenceFreshness.js` | Freshness bands and decay |
| `packages/scout/credibility/ContradictionAnalysis.js` | Numeric cross-source conflicts |
| `packages/scout/credibility/CredibilityFramework.js` | Intelligence brief builder |
| `packages/scout/investigation/InvestigationReport.js` | Attaches briefs to recommendations |
| `packages/scout/intelligence/IntelligenceReport.js` | Mission-level briefs |
| `packages/acquisition-mission/DiscoveryPayload.js` | AMO discovery credibility |
| `packages/acquisition-mission/DiscoveryPresentation.js` | Operator-facing brief rendering |

## Events

No new runtime events. Credibility is embedded in existing report deliverables (`investigation_report`, `mission_intelligence_report`, AMO discovery payload).

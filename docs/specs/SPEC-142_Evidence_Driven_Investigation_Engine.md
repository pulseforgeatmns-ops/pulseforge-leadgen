# SPEC-142 — Evidence-Driven Investigation Engine

| Field | Value |
|---|---|
| **Status** | Implemented (architectural foundation v1) |
| **Target Version** | v0.1 |
| **Priority** | High |
| **Owner** | Pulseforge |
| **Depends on** | [SPEC-141](SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md), [SPEC-100A](SPEC-100A_Scout_Acquisition_Discovery_Foundation.md) |

## Objective

Transform Scout from a sequential investigator into a hypothesis-driven intelligence system.

Scout should not execute a fixed pipeline. Scout should continually ask:

> What do I still need to know to confidently recommend an opportunity?

Every investigation becomes an iterative reasoning process rather than a static workflow.

## Philosophy

**Today (SPEC-141):**

```
Evidence Plan → Provider Strategy → Discovery → Qualification
```

**Future (SPEC-142):**

```
Hypothesis → Evidence Required → Evidence Collected → Evidence Missing
→ Next Best Investigation → Repeat
```

The investigation ends only when confidence reaches the mission threshold or no higher-value evidence remains.

Scout investigates uncertainty, not providers. Providers answer questions. Evidence supports claims. Claims build understanding. Understanding produces recommendations.

## Investigation Loop

Every investigation follows the same reasoning cycle:

```
Mission → Current Understanding → Generate Hypotheses → Determine Missing Evidence
→ Select Lowest-Cost Investigation → Collect Evidence → Fuse Evidence
→ Update Confidence → Enough Confidence?
  ├── Yes → Report
  └── No → Continue
```

## Investigation Graph

Scout builds an Investigation Graph connecting:

| Node | Role |
|---|---|
| Mission | Investigation objective |
| Market | Segment + geography definition |
| Candidate | Company under evaluation |
| Decision Maker | Person node linked to candidate |
| Evidence | Raw observation from a source |
| Claim | Asserted belief with confidence |
| Confidence | Score attached to a claim |
| Source | Provider that supplied evidence |

## Claims

Scout never says "ABC Property Management is a good target." Instead:

```json
{
  "text": "ABC Property Management manages multiple STR properties.",
  "confidence": 0.91,
  "supportedBy": ["website", "linkedin", "county_records", "google_reviews"],
  "missingEvidence": []
}
```

Every recommendation becomes explainable.

## Hypotheses

Before collecting evidence Scout creates hypotheses:

```json
{
  "text": "This company outsources cleaning.",
  "confidence": null,
  "requiredEvidence": ["hiring_activity", "vendor_references", "website", "reviews"],
  "minConfidence": 0.8
}
```

Hypotheses are first-class objects that drive what to investigate next.

## Missing Evidence

Scout knows exactly why confidence is low:

```json
{
  "currentConfidence": 0.56,
  "missing": ["decision_maker", "portfolio_size", "cleaning_responsibility"]
}
```

Instead of randomly searching, Scout searches with purpose.

## Investigation Planning

Rather than asking "Which provider?", Scout asks "What is the cheapest way to resolve this uncertainty?"

Example chain for `decision_maker`:

```
Website → LinkedIn → Apollo → Manual search
```

Dynamic replanning skips providers when earlier steps resolve the gap.

## Contradictions

Scout detects conflicting evidence and lowers confidence until resolved:

```
Website: "Family owned"  vs  LinkedIn: "350 employees" → Conflict detected → Investigation required
```

## Investigation Completion

Scout ends investigations when:

1. Coverage complete
2. Confidence threshold reached
3. No higher-value evidence remains
4. Cost exceeds benefit

## Deliverable — Investigation Report

```json
{
  "kind": "investigation_report",
  "missionIntelligence": {
    "market": "Greater Manchester STR",
    "coverage": 0.86,
    "claims": 18,
    "highConfidence": 14,
    "needsInvestigation": 4,
    "conflicts": 1,
    "recommendations": 6,
    "overallConfidence": 0.91
  },
  "claims": [],
  "hypotheses": [],
  "graph": {},
  "recommendations": []
}
```

## Six Questions (Acceptance Criteria)

For every recommendation Scout must answer:

1. **What do I believe?**
2. **Why do I believe it?**
3. **How confident am I?**
4. **What evidence supports it?**
5. **What evidence is still missing?**
6. **What is the next best investigation?**

If Scout cannot answer all six, the investigation is incomplete.

## Canonical Contract

```javascript
const { Scout } = require('@pulseforge/scout');

// Hypothesis-driven investigation engine (SPEC-142)
const result = await Scout.investigate({ mission, scoutPayload, opts });

// result.investigationReport — Investigation Report deliverable
// result.investigationGraph — connected graph of claims, evidence, sources
// result.hypotheses — first-class hypothesis objects
// result.recommendations[].sixQuestions — acceptance criteria answers
```

## Implementation Map

| Module | Role |
|---|---|
| `packages/scout/investigation/InvestigationLoop.js` | Hypothesis-driven loop orchestrator |
| `packages/scout/investigation/InvestigationGraph.js` | Connected graph of mission → claims → evidence |
| `packages/scout/investigation/HypothesisGeneration.js` | Generate hypotheses before evidence collection |
| `packages/scout/investigation/MissingEvidence.js` | Track gaps that block confidence |
| `packages/scout/investigation/InvestigationPlanner.js` | Select lowest-cost next investigation |
| `packages/scout/investigation/ClaimConfidence.js` | Confidence on claims, not providers |
| `packages/scout/investigation/ContradictionDetection.js` | Detect and penalize conflicting evidence |
| `packages/scout/investigation/EvidenceExecutor.js` | Execute dynamic investigation steps |
| `packages/scout/investigation/InvestigationReport.js` | Investigation Report + six questions |
| `packages/scout/investigation/types.js` | Investigation engine types |

## Relationship to SPEC-141

SPEC-141 remains the provider capability and evidence fusion foundation. SPEC-142 wraps those primitives in an iterative hypothesis loop. `Scout.investigate()` now runs the SPEC-142 engine, which reuses SPEC-141 market understanding, provider registry, evidence fusion, and qualification.

## Acceptance Criteria

- [x] Hypothesis-driven investigation loop (not fixed pipeline)
- [x] Investigation Graph with connected nodes
- [x] Claims with confidence, supportedBy, and missingEvidence
- [x] Hypothesis generation before evidence collection
- [x] Missing evidence tracking with purpose-driven search
- [x] Lowest-cost investigation selection with dynamic replanning
- [x] Contradiction detection lowers confidence until resolved
- [x] Completion when threshold met or no higher-value evidence remains
- [x] Investigation Report deliverable
- [x] Six questions answered for every recommendation
- [x] Tests in `test/scoutInvestigationEngine.test.js`

## Future Work

- Wire operational `leadgen.run()` through investigation engine
- Live provider dispatch from investigation planner
- Persist investigation graph to `acquisition_intelligence_state`
- EQL queries over investigation graph claims/evidence

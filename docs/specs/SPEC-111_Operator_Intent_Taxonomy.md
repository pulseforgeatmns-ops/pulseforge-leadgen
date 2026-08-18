# SPEC-111 — Operator Intent Taxonomy

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical — Anchor Pilot 0 |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-18 |
| **Depends on** | [SPEC-109](SPEC-109_Intent_Bound_Response_Selection.md), [SPEC-110](SPEC-110_Business_Intelligence_Synthesis.md), [SPEC-107](SPEC-107_Evidence_Grounded_Recommendation_Orchestration.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md) |
| **ADR** | [ADR-048 Intent Selects Analysis Mode](../adr/ADR-048_Intent_Selects_Analysis_Mode.md) |

## Objective

Expand Max's understanding of operator intent beyond Retrieval, Summary, and Recommendation.

Operators don't ask one kind of question. They ask to diagnose, compare, assess risk, identify unknowns, review progress, and understand change. Max should recognize these analytical modes **before reasoning begins**.

## Vision References

- [ADR-048 Intent Selects Analysis Mode](../adr/ADR-048_Intent_Selects_Analysis_Mode.md)
- [ADR-046 Intent Determines Response Structure](../adr/ADR-046_Intent_Determines_Response_Structure.md)
- [ADR-047 Intelligence Before Evidence](../adr/ADR-047_Intelligence_Before_Evidence.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)
- [ADR-039 Separate Understanding from Execution](../adr/ADR-039_Separate_Understanding_from_Execution.md)

## Problem

Current intent routing still collapses several fundamentally different operator questions into Recommendation or Blueprint Advisory.

| Operator | Current | Expected |
|---|---|---|
| What's preventing us from growing faster? | Recommendation | Diagnosis |
| What don't we know yet that matters? | Scout acquisition shortcut | Unknown Analysis |

Intent determines analysis. Analysis determines reasoning. Reasoning determines response.

## Design Principle

Max should recognize the kind of thinking the operator is requesting before reasoning begins.

Operators ask for different forms of intelligence. Max should respond with the corresponding form of analysis.

## Pipeline

```text
Current                         Proposed
Intent                          Intent
    ↓                               ↓
Recommendation                  Analysis Mode
                                    ↓
                                Response Contract
                                    ↓
                                Retrieve
                                    ↓
                                Ground
                                    ↓
                                Business Intelligence
                                    ↓
                                Compose
```

Intent should choose how Max thinks.

## Intent Registry

Intent classification is explicit:

- `RETRIEVAL`
- `SUMMARY`
- `RECOMMENDATION`
- `DIAGNOSIS`
- `UNKNOWN_ANALYSIS`
- `RISK`
- `PROGRESS`
- `CHALLENGE`
- `INVESTIGATION`

## Analysis Modes

| Mode | Purpose | Example questions | Output |
|---|---|---|---|
| **Retrieval** | Return verified operating state | What outreach has been sent? Show me Campaign 001. | Facts |
| **Summary** | Describe current business state | How is Anchor doing? Give me an overview. | BI → State → Goals → Unknowns |
| **Recommendation** | Determine the best next action | What should I do next? What should I focus on? | Recommendation → Reasoning → Evidence |
| **Diagnosis** | Identify the limiting constraint | What's preventing growth? What's the bottleneck? Where are we stuck? Why aren't we growing? | Current bottleneck → Supporting evidence → Confidence → Operator impact |
| **Unknown Analysis** | Identify uncertainty | What don't we know? What's missing? What assumptions remain? | Critical unknowns → Evidence gaps → Why they matter → Suggested investigations |
| **Risk Assessment** | Identify threats | What's risky? Where could this fail? What worries you? | Risks → Evidence → Confidence → Potential impact |
| **Progress Review** | Measure movement | How are we progressing? What's improved? What's completed? | Progress → Remaining work → Confidence |
| **Challenge** | Review disputed claims | That's incorrect. | Already exists |
| **Investigation** | Determine whether specialist work is required | Investigate commercial prospects. | Already exists |

Diagnosis explains **why**, not what to do. Recommendations may appear afterward as optional follow-up.

Unknown Analysis does not speculate.

## Business Intelligence Integration

Business Intelligence objects are reusable inputs. No duplicated reasoning.

| Mode | Consumes |
|---|---|
| Diagnosis | `bottleneck`, `readiness`, `momentum` |
| Unknown Analysis | `unknown` findings |
| Risk | `risk` findings |
| Progress | `momentum`, goal-versus-observed unknowns |

## Response Contracts

Each analysis mode owns its contract.

**Diagnosis**

- Required: bottleneck, confidence, evidence
- Optional: recommendation
- Forbidden: generic Blueprint strategy

**Unknown Analysis**

- Required: unknowns, impact, evidence gaps
- Forbidden: speculation

**Risk**

- Required: risks, evidence, confidence, potential impact
- Forbidden: speculation, generic Blueprint strategy

**Progress**

- Required: progress, remaining work, confidence
- Forbidden: generic Blueprint strategy

## Scope

1. Explicit operator intent registry (`OperatorIntentRegistry.js`)
2. Analysis-mode classification before recommendation or specialist routing
3. Diagnosis, Unknown Analysis, Risk, and Progress response contracts
4. Business Intelligence consumption by analysis mode (no duplicated reasoning)
5. Scout must not swallow "what don't we know" as an acquisition follow-up
6. Competency Registry — `operator_intent_taxonomy`

## Out of Scope

- New recommendation engine
- Persisting intelligence objects as operating fact
- Workspace UI section rendering for new contract headings
- Autonomous execution
- Durable assimilation of operator corrections

## Architecture

Exact integration extends:

- `OperatorIntentRegistry` — nine-intent taxonomy, diagnosis / unknown / risk / progress classifiers
- `CognitiveMode` — classifies new modes before recommendation; never-delegate for analytical modes
- `ResponseContract` — `DiagnosisContract`, `UnknownAnalysisContract`, `RiskContract`, `ProgressContract`
- `BusinessIntelligence` — `selectForQuestion` consumes existing objects by mode
- `OperatingEvidenceRetrieval` — compose according to the selected analytical contract
- `ScoutAcquisitionContext` / `NeedAssessment` — unknown analysis is not a Scout shortcut
- Competency Registry — `operator_intent_taxonomy`

## Data Model

No new tables. Analysis mode is ephemeral response metadata (`analysisMode`, `operatorIntent`, `responseContract`). Intelligence objects remain composed at response time (ADR-047).

## Implementation Plan

1. Registry + classification
2. Contracts + composition
3. BI consumption
4. Scout / CIE isolation
5. Acceptance tests + competency graduation

## Migration Strategy

None. Classification and composition only.

## Testing

- `packages/max/workspace/tests/operatorIntentTaxonomy.test.js`
- `test/operatorIntentTaxonomy.test.js`

## Acceptance Criteria

- [x] Diagnosis (`What's preventing us from growing faster?`) identifies execution as the bottleneck. No generic commercial acquisition advice.
- [x] Unknown Analysis (`What don't we know yet that matters?`) returns conversions, walkthroughs, Yelp performance, and campaign execution. Not acquisition rumors.
- [x] Risk (`What's our biggest operational risk?`) returns grounded risks only.
- [x] Progress (`How much progress have we made?`) measures progress against goals.
- [x] Recommendation (`What should we do next?`) remains recommendation-first. No regression.
- [x] `operator_intent_taxonomy` registered and graduated in the Competency Registry

## Future Work

- Comparison as a first-class mode if operator questions need explicit A-vs-B structure
- Workspace UI rendering of diagnosis / unknown / risk / progress headings
- Rex / briefing consumers selecting objects by analysis mode

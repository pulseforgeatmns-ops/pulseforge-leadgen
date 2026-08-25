# AUDIT-044 — Discovery Coverage & Investigative Reasoning

| Field | Value |
|---|---|
| **Status** | Completed |
| **Date** | 2026-08-25 |
| **Related** | [SPEC-153](../specs/SPEC-153_Discovery_Coverage_Engine.md), [SPEC-141](../specs/SPEC-141_Scout_Intelligence_Pipeline.md), [SPEC-142](../specs/SPEC-142_Scout_Investigation_Engine.md), [SPEC-123](../specs/SPEC-123_Unified_Scout_Discovery_Pipeline.md), [ADR-076](../adr/ADR-076_Coverage_Before_Conclusion.md), [AUDIT-006](AUDIT-006_Scout_Discovery_Execution_Audit.md) |
| **Scope** | Verify Scout does not conclude market discovery until sufficient evidence exists that the target market was investigated. Evaluates **reasoning quality**, not search vendor implementation. |
| **Principle** | **ADR-076 — Discovery Is Investigation:** Discovery is not the act of searching. Discovery is the process of reducing uncertainty about a market until Max can make evidence-based business decisions. |

## Executive summary

| Verdict | Path |
|---|---|
| **PASS (conditional)** | `Scout.investigate()` + `runScoutAcquisitionIntelligence()` with coverage engine enabled |
| **FAIL** | Mission Engine default `Scout.discover()` → `prospect_discovery` on `EXTERNAL_HEAVY` strategy |
| **FAIL** | Operational cron Scout (`leadgen.js`) |

Scout **does** implement structured discovery coverage (SPEC-153), terminology expansion, geographic cluster execution, empty-universe gating, and Mission Intelligence Reports — but on **two parallel paths** that are not fully unified. The coverage engine and investigative reasoning live on the acquisition-intelligence and `Scout.investigate()` paths. The Mission Engine's default Discovery stage still routes through `prospect_discovery` (single-profile external search) and only invokes the coverage engine opportunistically when strategy is `HYBRID` or `VERIFICATION_ONLY`.

**Automated evidence:** `packages/acquisition-mission/tests/spec153DiscoveryCoverage.test.js` — 8/8 passing. `test/scoutIntelligencePipeline.test.js` and `test/scoutInvestigationEngine.test.js` — 22/22 passing.

---

## Scout execution paths under audit

```
Mission Engine (Discovery stage)
  └─ ScoutDiscoveryExecutor
       └─ Scout.discover() [SPEC-123]
            ├─ prospect_discovery capability [SPEC-024]  ← default external path
            └─ runScoutAcquisitionIntelligence()         ← HYBRID / VERIFICATION_ONLY only

Scout.investigate() [SPEC-141/142]
  └─ Intelligence Pipeline + Investigation Loop
       └─ constructCandidateUniverse() + DiscoveryCoverageEngine [SPEC-153]
```

| Path | Coverage engine | Pre-search universe estimate | Stopping rationale | Intelligence report |
|---|---|---|---|---|
| `Scout.investigate()` | Yes | Partial (post-hoc heuristic) | Yes (InvestigationJournal) | Yes |
| `runScoutAcquisitionIntelligence()` | Yes (default) | Partial | Yes (discoveryReport) | Yes (via payload) |
| `Scout.discover()` → `prospect_discovery` | **No** | **No** | Partial (execution report only) | **No** |
| `leadgen.js` cron Scout | **No** | **No** | **No** | **No** |

---

## Stage-by-stage audit

Reference mission used for live trace: *"Find short-term rental operators in Greater Manchester"* (`client_id=1`, vertical `short_term_rental`).

### Stage 1 — Mission Understanding

**Requirement:** Target segment, geography, cities, industry, known synonyms, expected decision makers, success criteria. Fail immediately if any missing.

| Field | Status | Evidence |
|---|---|---|
| Target segment | **PASS** | `buildMarketDefinition()` → `segment`, `segments` |
| Target geography | **PASS** | `geography` from delegation / constraints |
| Cities | **PARTIAL** | Explicit city list only after coverage expansion; not in Stage 1 output |
| Industry | **PASS** | `industry` from constraints.vertical |
| Known synonyms | **PARTIAL** | Expanded later via `ConceptLibrary`; not surfaced in market definition |
| Expected decision makers | **PARTIAL** | `EvidencePlanning` adds `decision_makers` requirement; not in Stage 1 artifact |
| Success criteria | **FAIL** | Not extracted or persisted in market definition |

**Module:** `packages/scout/intelligence/MarketUnderstanding.js`

**Finding:** Stage 1 passes for core segment/geography/industry but **fails the audit's "fail immediately if any missing" bar** on cities, synonyms, decision makers, and success criteria as first-class mission-understanding outputs.

---

### Stage 2 — Candidate Universe Estimation

**Requirement:** Before searching, estimate minimum / expected / maximum / confidence (e.g. STR operators: min 35, expected 60, max 95, confidence 0.62).

| Criterion | Status | Evidence |
|---|---|---|
| Pre-search estimation | **FAIL** | No min/expected/max/confidence struct exists |
| Post-search heuristic | Partial | `discoverCandidateUniverse()` uses `Math.max(discovered, Math.round(discovered / 0.7))` after discovery |
| Investigation plan estimate | Partial | `InvestigationPlanBuilder.estimatePlanMetrics()` sets `estimatedUniverse` only when passed via opts |

**Finding:** Scout **never estimates the candidate universe before searching** in the format AUDIT-044 requires. Coverage ratios are therefore computed against a derived post-hoc number, not an operator-visible benchmark established upfront.

---

### Stage 3 — Investigation Plan

**Requirement:** Explicitly state search sources — CRM, Google Places, property managers, vacation rental managers, Airbnb, VRBO, Facebook, LinkedIn, existing memory, local directories, manual evidence. Nothing hidden.

| Source (audit list) | Wired as discovery adapter | Notes |
|---|---|---|
| CRM / existing memory | **Yes** | `existing_pf_company_intelligence` via `loadRepository()` |
| Google Places | **Yes** | `public_business_data` / PlacesProvider |
| Property managers | Via concept expansion | Searched as "Property Manager" concept, not separate source |
| Vacation rental managers | Via concept expansion | "Vacation Property Manager" concept |
| Airbnb | **No** | Not a distinct source adapter |
| VRBO | **No** | Not a distinct source adapter |
| Facebook | **Partial** | `facebook_social_intelligence` type exists; not in default coverage plan adapters |
| LinkedIn | **Partial** | `linkedin_social_intelligence` type exists; not in default coverage plan adapters |
| Local directories | **No** | Not implemented |
| Manual evidence | **No** | Not in automated plan |

**Default STR plan (simulated):** 1 source (`public_business_data`), 6 cities × 6 concepts = **36 workloads**.

**Modules:** `InvestigationPlanBuilder.js`, `EvidencePlanning.js`, `DiscoveryCoverageEngine.buildDiscoveryPlan()`

**Finding:** Investigation planning is explicit on the `Scout.investigate()` path. The coverage-engine path enumerates workloads but **does not expose the full source matrix** AUDIT-044 expects. Social and platform-specific sources (Airbnb, VRBO, Facebook groups) are absent from default execution.

---

### Stage 4 — Geographic Coverage

**Requirement:** For every mission city — searched?, candidate count, confidence, coverage.

| City | In plan (Greater Manchester) | Per-city metrics in engine |
|---|---|---|
| Manchester | **Yes** | Workload-level `resultCount` in `executed[]` |
| Bedford | **Yes** | Same |
| Hooksett | **Yes** | Same |
| Londonderry | **Yes** | Same |
| Auburn | **Yes** | Same |
| Goffstown | **Yes** | Same |

**Module:** `DiscoveryCoverageEngine.expandCitiesFromSearchDefinition()` — expands "Greater Manchester" to 6 NH cities via `MANCHESTER_GEO`.

**Aggregate metrics:** `coverage.cities.searched / coverage.cities.planned` with ratio.

**Finding:** **PASS** for cluster expansion and measurability. **PARTIAL** for operator-facing per-city confidence/coverage panel — raw data exists in `executed[]` but is not rendered as the audit's city table in AMO presentation.

---

### Stage 5 — Terminology Expansion

**Requirement:** Search beyond literal mission wording. Capture search term, results, reason kept, reason discarded.

**Configured STR concepts** (`ConceptLibrary.js`):

| Search term | In library | Audit example term |
|---|---|---|
| STR | Yes | Short-term rental |
| Vacation Rental | Yes | Vacation rental |
| Airbnb Host | Yes | Airbnb host |
| Vacation Property Manager | Yes | Airbnb management |
| Property Manager | Yes | Property management |
| Hospitality Operator | Yes | Boutique hospitality |
| Guest services | **No** | Guest services |
| Corporate housing | **No** | Corporate housing |
| Executive stays | **No** | Executive stays |
| VRBO | Detected in text, not separate concept | VRBO |

**Per-term audit trail:** **FAIL** — no `reasonKept` / `reasonDiscarded` / per-term result counts in operator artifacts.

**Finding:** Terminology expansion **works** for the configured set but is ** narrower than the audit example** and lacks per-term reasoning capture.

---

### Stage 6 — Search Expansion

**Requirement:** Failed searches generate another investigation branch; never terminate immediately.

| Behavior | Status | Evidence |
|---|---|---|
| Zero results on one source → continue others | **PASS** | `executeCoveragePlan()` iterates all workloads |
| Failure classification (ambiguous geo, wrong terminology, source exhausted, API unavailable) | **PARTIAL** | `status: failed/skipped` + `reason` on workload; no structured taxonomy |
| Hypothesis branch on investigate path | **PASS** | `InvestigationLoop` + `HypothesisGeneration` |
| prospect_discovery single-pass | **FAIL risk** | May conclude from one profile query without coverage plan |

**Finding:** Coverage engine and investigation loop satisfy the "never stop after one search" invariant. The `prospect_discovery`-only path does **not**.

---

### Stage 7 — Coverage Score

**Requirement:** Per-source coverage (CRM, Google, Facebook, LinkedIn, Airbnb, VRBO, Directories, Previous memory) + Overall coverage % + Confidence %.

**Implemented metrics** (`computeCoverageMetrics`, `computeDiscoveryConfidence`):

| Dimension | Tracked |
|---|---|
| Cities | searched / planned / ratio |
| Concepts | searched / planned / ratio |
| Sources | searched / planned / ratio |
| Searches | executed / addressed / planned |
| Overall confidence | Weighted: coverage 45%, searchSuccess 20%, diversity 20%, evidenceQuality 15% |

**Per-source breakdown (audit format):** **PARTIAL** — source types aggregate; Airbnb/VRBO/Facebook not individually scored.

**Finding:** **PASS** for dimensional coverage scoring on the SPEC-153 path. **PARTIAL** for the audit's named source matrix.

---

### Stage 8 — Stopping Decision

**Requirement:** Scout must explain *why* it stopped — not "I found zero prospects."

| Stop reason | Implemented | Path |
|---|---|---|
| Coverage exceeded threshold | Yes | `COMPLETION_REASONS.COVERAGE_COMPLETE` |
| All discovery branches exhausted | Yes | `NO_HIGHER_VALUE_EVIDENCE`, diminishing returns |
| Operator threshold reached | Yes | Via mission approval gates |
| API limits reached | Partial | `adapter_unavailable`, budget exhausted |
| Manual investigation required | Partial | `discoveryReport.recommendation` |

**Module:** `InvestigationJournal.recordJournalStop()`, `canConcludeEmptyUniverse()`

**Finding:** **PASS** on `Scout.investigate()` and acquisition-intelligence paths. **FAIL** on default `Scout.discover()` EXTERNAL_HEAVY path — stopping rationale not propagated to operator as structured investigation conclusion.

---

### Stage 9 — Empty Market Validation

**Requirement:** 0 qualified prospects requires coverage ≥ threshold AND all branches exhausted; otherwise output "Discovery Incomplete."

| Invariant | Status | Module |
|---|---|---|
| Incomplete coverage blocks empty conclusion | **PASS** | `discoveryStatusFromCoverage()` → `'incomplete'` |
| Complete coverage + zero qualified = valid empty | **PASS** | `canConcludeEmptyUniverse()` |
| Prioritization blocked on incomplete | **PASS** | `hasSufficientEvidenceForPrioritization()` returns false when `discoveryStatus === 'incomplete'` |
| prospect_discovery bypass | **FAIL risk** | Path does not run coverage engine on EXTERNAL_HEAVY |

**Test evidence:** Scenario 5 and 6 in `spec153DiscoveryCoverage.test.js`.

**Finding:** Empty-market validation is **correctly enforced** where the coverage engine runs. The Mission Engine default path can still reach "zero prospects" without coverage proof.

---

### Stage 10 — Intelligence Report

**Requirement:** Mission Intelligence Report with estimated market, investigated count, coverage %, qualified breakdown, missing evidence, confidence, recommendation.

| Field | `Scout.investigate()` | Coverage engine (`buildDiscoveryReport`) | AMO presentation |
|---|---|---|---|
| Estimated market | Yes (`estimatedUniverse`) | Partial (candidateUniverse count) | Yes (`candidateUniverseCount`) |
| Investigated | Yes | Via executed searches | Partial |
| Coverage % | Yes | Via ratios | Yes (coverage panel) |
| Qualified breakdown | Yes (strong/immediate/watch) | qualified count only | rankedProspects |
| Missing evidence | Yes (`remainingUnknowns`) | warnings array | Partial |
| Confidence | Yes | Yes (`discoveryConfidence.overall`) | Yes |
| Recommendation | Yes | Yes | Yes |

**Module:** `IntelligenceReport.js`, `DiscoveryPresentation.js`

**Finding:** **PASS** on intelligence pipeline path. **PARTIAL** on unified `Scout.discover()` — report depends on whether `runScoutAcquisitionIntelligence` was invoked.

---

## Acceptance criteria verdict

| Scout fails if… | Result |
|---|---|
| Stops after one search | **PASS** (coverage engine / investigate); **FAIL** (prospect_discovery-only) |
| Treats "0 results" as evidence market is empty | **PASS** (gated); **FAIL risk** (default path) |
| Never estimates candidate universe | **FAIL** — no pre-search min/expected/max |
| Never measures coverage | **PASS** on SPEC-153 path; **FAIL** on prospect_discovery-only |
| Never expands terminology | **PASS** (ConceptLibrary) |
| Never expands geography | **PASS** (Greater Manchester → 6 cities) |
| Never explains why investigation stopped | **PASS** (investigate path); **PARTIAL** (discover path) |
| Reports confidence without supporting evidence | **PASS** — confidence derived from coverage ratios |
| Conflates "searched" with "understood" | **PARTIAL** — market definition lacks success criteria / decision-maker contract |

**Overall: CONDITIONAL FAIL** — Core investigative reasoning exists but is **not consistently applied** on the path operators trigger from Mission Engine Discovery approval.

---

## Recommended remediation (priority order)

1. **Unify discovery paths (AUDIT-006 follow-up)** — Route `Scout.discover()` Phase 3 through `runScoutAcquisitionIntelligence()` with `useCoverageEngine: true` for all external strategies, not only HYBRID/VERIFICATION_ONLY.
2. **Pre-search universe estimation** — Add `estimateCandidateUniverse(marketDefinition)` returning `{ minimum, expected, maximum, confidence }` before `buildDiscoveryPlan()`.
3. **Stage 1 completeness** — Extend `buildMarketDefinition()` to emit cities, synonym set, expected decision makers, and success criteria as required fields.
4. **Source matrix expansion** — Register optional adapters for social/platform sources; include in plan with `skipped/unavailable` status rather than omission.
5. **Terminology audit trail** — Persist per-concept workload results (`resultCount`, `status`, `discardReason`) in discovery payload for operator review.
6. **ADR-076 document** — Formalize "Coverage Before Conclusion" (see `docs/adr/ADR-076_Coverage_Before_Conclusion.md`).
7. **Operational Scout** — `leadgen.js` remains out of scope for AMO discovery; document as legacy path or migrate to coverage engine.

---

## Appendix — Simulated STR Greater Manchester plan

```
Stage 1: segment=short term rental, geography=Greater Manchester, industry=short_term_rental
Stage 4: 6 cities (Manchester, Bedford, Goffstown, Hooksett, Londonderry, Auburn NH)
Stage 5: 6 concepts (STR, Vacation Rental, Airbnb Host, Vacation Property Manager, Property Manager, Hospitality Operator)
Stage 3/7: 1 source × 6 cities × 6 concepts = 36 planned searches
Stage 2: No pre-search universe estimate emitted
```

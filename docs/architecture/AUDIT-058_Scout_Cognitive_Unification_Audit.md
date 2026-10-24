# AUDIT-058 — Scout Cognitive Unification Audit

| Field | Value |
|---|---|
| **Status** | Completed |
| **Date** | 2026-08-25 |
| **Related** | [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md), [AUDIT-006](AUDIT-006_Scout_Discovery_Execution_Audit.md), SPEC-141–177 |
| **Scope** | Map every Scout entry point against the canonical cognitive pipeline; identify duplicate cognition paths |

## Executive Summary

The Scout cognitive architecture **exists** (SPEC-141 through SPEC-177) but is **not unified**. Multiple parallel orchestrators, hypothesis engines, investigation planners, provider registries, and explainability structures coexist. Production cron Scout (`leadgen.js`) bypasses the entire cognitive stack.

**Verdict:** EPIC-001 work is elimination of duplicate cognition, not net-new intelligence.

---

## Canonical Pipeline (Target State)

```
Mission Objective
  ↓
Market Definition          (SPEC-178)
  ↓
Hypothesis Engine          (SPEC-179)
  ↓
Investigation Planner      (SPEC-180)
  ↓
Evidence Requirements
  ↓
Provider Capability Planner (SPEC-182)
  ↓
Evidence Collection        (SPEC-181)
  ↓
Understanding + Judgment
  ↓
Recommendation
  ↓
Explainability Graph       (SPEC-183)
```

---

## Entry Point Audit

| Entry Point | Uses Canonical Pipeline? | Cognition Path | Gap |
|---|---|---|---|
| `Scout.discover()` → `runDiscoveryPipeline()` | **Yes** | Full 7-stage pipeline + SPEC-159 reasoning loop | Production path for Mission Engine / AMO |
| `runIntelligencePipeline()` (SPEC-141) | No | Separate 8-stage orchestrator | **Tests only** — not wired to production |
| `runInvestigationEngine()` via `investigate()` (SPEC-142) | No | Separate investigation loop | **Deprecated export** — tests only |
| `CandidateUniverse.constructCandidateUniverse()` | Partial | 3-way branch (SPEC-177 / SPEC-158 / SPEC-153) | **Duplicate cognition** when `marketDefinition` present |
| `leadgen.js` (cron Scout) | **No** | Direct SerpAPI + Places + Prospeo | **Critical gap** — provider-first, no hypotheses |
| `scoutPublicSourcing.js` (SPEC-077) | **No** | Direct Places Text Search | Parallel public-source path |
| `ProspectDiscovery` capability (SPEC-024) | **No** | Profile-based Places search | Legacy capability still registered |
| `pilotScout.js` (SPEC-115) | Partial | Via `runScoutAcquisitionIntelligence` | Uses CandidateUniverse branches |

---

## Duplicate Cognition Inventory

### 1. Three Top-Level Orchestrators

| Orchestrator | File | Production? |
|---|---|---|
| `runDiscoveryPipeline` | `packages/scout/DiscoveryPipeline.js` | **Yes** |
| `runIntelligencePipeline` | `packages/scout/intelligence/Pipeline.js` | No (tests) |
| `runInvestigationEngine` | `packages/scout/investigation/InvestigationLoop.js` | No (deprecated) |

**Remediation (EPIC-001 Phase 4):** Retire or fold unique stages from non-canonical orchestrators into `DiscoveryPipeline`.

### 2. Three Discovery Execution Branches in CandidateUniverse

Location: `packages/max/scoutAcquisition/CandidateUniverse.js:274–314`

| Branch | Trigger | Spec | Status |
|---|---|---|---|
| `runHypothesisDrivenDiscovery` | `marketDefinition && useHypothesisDiscoveryEngine !== false` | SPEC-177 | **Default — keep** |
| `executeHypothesisDrivenCoverage` | `marketDefinition && useHypothesisEngine !== false` | SPEC-158 | **Duplicate — retire as branch** |
| `executeCoveragePlan` | else | SPEC-153 | Legacy provider-first — keep only when no `marketDefinition` |

**Remediation (EPIC-001 Phase 1):** When `marketDefinition` is present, always route through SPEC-177. Merge SPEC-158 terminology hypotheses into `CanonicalHypothesisEngine` (SPEC-179).

### 3. Three Hypothesis Engines

| Engine | File | Hypothesis Type |
|---|---|---|
| Business hypotheses | `investigation/HypothesisGeneration.js` | ICP gaps (portfolio, decision maker, cleaning) |
| Terminology hypotheses | `investigation/SearchHypothesisEngine.js` | Market self-description |
| Vertical search strategies | `hypothesis/MarketHypothesisRegistry.js` | Static query templates (`leadgen.js` only) |

**Remediation (SPEC-179):** Single `CanonicalHypothesisEngine` emitting typed hypothesis nodes (`business`, `terminology`, `search_strategy`).

### 4. Two Investigation Planners

| Planner | Version | File |
|---|---|---|
| InvestigationPlanBuilder | SPEC-145 | `investigation/InvestigationPlanBuilder.js` |
| HypothesisInvestigationPlanner | SPEC-177 | `coverage/HypothesisInvestigationPlanner.js` |

Both call `generateHypotheses()` but produce different plan shapes.

**Remediation (SPEC-180):** `HypothesisInvestigationPlanner` becomes canonical; SPEC-145 delegates or is deprecated.

### 5. Two Provider Registries

| Registry | Purpose | File |
|---|---|---|
| ProviderCapabilityRegistry | Evidence collection cost optimization | `intelligence/ProviderCapabilityRegistry.js` |
| ExternalDiscoveryProviderRegistry | Discovery capability states | `coverage/ExternalDiscoveryProviderRegistry.js` |

**Remediation (SPEC-182):** Unified provider capability model with evidence-type assignment and availability state.

### 6. Fragmented Explainability

| Structure | File | Scope |
|---|---|---|
| InvestigationGraph | `investigation/InvestigationGraph.js` | SPEC-142 evidence graph |
| InvestigationTree | `investigation/InvestigationTree.js` | SPEC-158 branch lineage |
| MemoryGraph | `memory/MemoryGraph.js` | SPEC-143 market memory |
| Explainability (provenance) | `max/scoutAcquisition/Explainability.js` | SPEC-100 provenance chain |
| ScoutDiscoveryArtifact | `adapters/ScoutDiscoveryArtifact.js` | SPEC-172 AMO handoff |

**Remediation (SPEC-183):** Single `ExplainabilityGraph` projecting to operator UI and AMO boundary.

### 7. Production Cron Bypass

Location: `leadgen.js:1472–1489`

Direct SerpAPI + Google Places loops with no `Scout.discover()`, no hypothesis engine, no investigation plan.

ADR-092 open item: *Full `leadgen.js` refactor to delegate search planning to `HypothesisDrivenDiscovery`*.

**Remediation (EPIC-001 Phase 3):** Operational persistence adapter wrapping canonical pipeline.

---

## Traceability Test

For each recommendation Scout produces, verify the chain:

```
Recommendation → Judgment → Understanding → Evidence → Hypothesis → Mission Objective
```

| Path | Traceable? | Notes |
|---|---|---|
| Mission Engine / AMO via `Scout.discover()` | **Partial** | Artifact handoff (SPEC-172) exists; unified graph missing |
| Cron Scout via `leadgen.js` | **No** | Recommendation = ICP score; no hypothesis or evidence chain |
| SPEC-077 public sourcing | **No** | Review-only candidates; no judgment layer |

---

## Recommended Remediation Order

1. **Phase 1** — Unify `CandidateUniverse` branches + `CanonicalHypothesisEngine`
2. **Phase 2** — `ExplainabilityGraph` consolidation
3. **Phase 3** — `leadgen.js` migration (highest business impact)
4. **Phase 4** — Legacy orchestrator retirement

---

## Acceptance Criteria for AUDIT-058 Close

- [x] Every Scout entry point documented with cognition path
- [x] Duplicate paths enumerated with file references
- [x] Remediation mapped to SPEC-178–183
- [ ] Phase 1 code changes merged (CandidateUniverse unification)
- [ ] Phase 3 code changes merged (`leadgen.js` migration)

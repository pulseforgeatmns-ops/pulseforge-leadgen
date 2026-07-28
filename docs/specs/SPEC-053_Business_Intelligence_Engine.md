# SPEC-053 — Business Intelligence Engine

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | High |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-28 |
| **Completed** | 2026-07-28 |
| **Depends on** | SPEC-023, SPEC-031, SPEC-042, SPEC-048; soft-depends SPEC-030 (facts), SPEC-001 (Knowledge Graph) |
| **Blocks** | Richer Sales Intelligence inputs; messaging provenance to business reasoning |
| **ADR** | [ADR-037 Reason About Businesses, Not Companies](../adr/ADR-037_Reason_About_Businesses_Not_Companies.md) |
| **Supersedes (reasoning scope)** | Descriptive Company Intelligence as the first reasoning artifact ([SPEC-030](SPEC-030_Company_Intelligence_Capability.md) remains the contact/firmographic enrichment contract) |

## Objective

Replace descriptive Company Intelligence with analytical **Business Intelligence**.

The system should understand **how a business operates**—not merely describe what it is.

Success looks like: every ranked prospect receives a `BusinessIntelligenceProfile` that answers how they make money, what constrains growth, what operational pressures exist, who owns the problem, and why they would buy now — with explicit confidence and uncertainty — before Sales Intelligence derives strategy.

## Vision References

- [ADR-037 Reason About Businesses, Not Companies](../adr/ADR-037_Reason_About_Businesses_Not_Companies.md)
- [ADR-017 Intelligence Before Execution](../adr/ADR-017_Intelligence_Before_Execution.md)
- [ADR-032 Strategy Before Language](../adr/ADR-032_Strategy_Before_Language.md)
- [ADR-002 Explainable AI](../adr/ADR-002_Explainable_AI.md)
- [ADR-005 LLM Presentation Engine](../adr/ADR-005_LLM_Presentation_Engine.md)
- [ADR-031 Review Must Be Evidence-First](../adr/ADR-031_Review_Must_Be_Evidence_First.md)
- [SPEC-030 Company Intelligence](SPEC-030_Company_Intelligence_Capability.md) (facts / enrichment — soft)
- [SPEC-031 Business Signals](SPEC-031_Business_Signals_Capability.md)
- [SPEC-048 Sales Intelligence Engine](SPEC-048_Sales_Intelligence_Engine.md)
- [SPEC-042 Mission Artifact Bus](SPEC-042_Mission_Artifact_Bus.md)

## Problem

Current Company Intelligence answers:

> What exists?

Sales Intelligence needs answers to:

> Why does this business behave this way?

Descriptive packages (industry, employees, website) hide operational meaning. Operators cannot inspect business reasoning before strategy and language.

## Philosophy

```text
Facts become understanding.
Understanding becomes strategy.
Strategy becomes messaging.
```

## Pipeline

### Current

```text
Prospect → Company Intelligence → Sales Intelligence
```

### New

```text
Prospect → Business Intelligence → Sales Intelligence → Campaign
```

Mission seed (v1):

```text
ProspectList → Company Enrichment (facts stub)
  → Opportunity Ranking
  → ★ BusinessIntelligenceProfile ★
  → SalesIntelligenceProfile
  → Campaign → Mail Package → Review
```

## Business Intelligence Profile

| Field | Notes |
|---|---|
| `prospectId` | Stable id |
| `company` | Company name |
| `industry` | Evidenced industry |
| `business_model` | e.g. Professional services |
| `revenue_model` | How money is made |
| `primary_customers` | Who pays |
| `growth_strategy` | How they expand |
| `competitive_position` | Relative posture (explicit uncertainty when unknown) |
| `operational_constraints` | Capacity / overhead constraints |
| `likely_kpis` | Outcomes management tracks |
| `cost_drivers` | What consumes spend / attention |
| `risk_factors` | Material risks |
| `buying_triggers` | What creates purchase urgency |
| `decision_makers` | Likely problem owners |
| `vendor_landscape` | Facility / ops vendor posture |
| `seasonality` | Timing when known |
| `expansion_signals` | Growth / change signals |
| `qualityAnswers` | Structured answers to the five required questions |
| `uncertainty` | Explicit unanswered / low-confidence items |
| `service_angle` | Cleaning / client relevance angle (playbook-framed) |
| `confidence` / `confidenceScore` | High / Medium / Low + numeric |
| `evidenceRefs` | Provenance |
| `reasoningLayers` | Level 1–5 summary for review |

## Required Reasoning

The engine should answer:

1. How does this company make money?
2. What constrains growth?
3. Where is operational leverage?
4. What consumes management attention?
5. What would cause them to purchase services?
6. What business outcomes matter most?

### Quality gates (must answer)

| Question | Profile field |
|---|---|
| How do they make money? | `qualityAnswers.howTheyMakeMoney` / `revenue_model` |
| What likely constrains growth? | `qualityAnswers.growthConstraints` / `operational_constraints` |
| What operational pressures exist? | `qualityAnswers.operationalPressures` |
| Who likely owns the problem? | `qualityAnswers.problemOwner` / `decision_makers` |
| Why would they buy now? | `qualityAnswers.whyBuyNow` / `buying_triggers` |

If those cannot be answered confidently, append to `uncertainty` rather than inventing.

### Example

Instead of:

```text
Industry: Law Firm
Employees: 14
```

Produce:

```text
Business Model: Professional services.
Revenue Driver: Billable attorney time.
Operational Constraint: Administrative overhead reduces billable utilization.
Buying Trigger: Growth in headcount.
Likely KPI: Attorney utilization and client satisfaction.
Cleaning Angle: Reliable facility operations protect productive billable hours.
```

## Business Reasoning Layers

| Level | Name | Content |
|---|---|---|
| 1 | Facts | Website, employees, location, industry, signals |
| 2 | Business Model | Professional Services / Healthcare / Manufacturing / Retail / … |
| 3 | Operational Model | Revenue generation, daily operations, capacity, growth mechanics |
| 4 | Buying Psychology | Urgency, risks, outcomes that matter |
| 5 | Sales Intelligence Input | Messaging priorities, CTA seeds, value proposition, objection prediction |

## Scope (v1)

- Package `packages/capabilities/businessIntelligence/`
  - `types.js` — `BusinessIntelligenceProfile`, quality answers, confidence
  - `reason.js` — deterministic Level 1–5 reasoning (no LLM inventing facts)
  - `gates.js` — quality gates with explicit uncertainty
  - `BusinessIntelligenceEngine.js` — capability façade
  - `index.js`
- Artifact Bus type `BusinessIntelligenceProfile` / alias `business_intelligence_profile`
- Stage `business_intelligence` in Stage Library + PipelineGate contract
- Sales Intelligence consumes BI profiles (falls back to thin prospect facts with lower confidence)
- Review Workspace: Business Intelligence → Sales Intelligence → Messaging Strategy → Mail Package
- Mail packages carry `businessIntelligence` for review provenance
- Tests under `packages/capabilities/tests/`

## Out of Scope (v1)

- Live SPEC-030 deep enrichment (contacts without fabrication)
- LLM freeform business essays without structured profile
- Replacing Opportunity Ranking or Client Playbook
- Durable Knowledge Graph research crawlers
- Replacing Sales Intelligence

## Output Contract

| | |
|---|---|
| **Produces** | `BusinessIntelligenceProfile` |
| **Consumes** | Prospect, company/enrichment facts (optional), research/signals, Knowledge Graph (optional), Client Playbook (optional) |

## Architecture

```text
Level 1 Facts
     ↓
Level 2 Business Model
     ↓
Level 3 Operational Model
     ↓
Level 4 Buying Psychology
     ↓
Level 5 Sales Intelligence Input
     ↓
BusinessIntelligenceProfile → Sales Intelligence → Messaging → Channel Generator
```

## Implementation Plan

1. Types + reason + gates + capability
2. Artifact Registry / Stage Library / PipelineGate / contracts
3. Wire Sales Intelligence + Mail Package attachment
4. Review Workspace presentation order
5. Docs: SPEC, ADR, CHANGELOG, CURRENT_STATE, indexes

## Migration Strategy

- Additive stage; existing missions gain BI when seeded for campaign / discovery types
- `company_enrichment` remains fact enrichment (operator label no longer “Company Intelligence” as the reasoning surface)
- Sales Intelligence remains compatible with missing BI (lower confidence + uncertainty)

## Testing

- Unit: industry → business model / revenue / constraints / buying triggers
- Unit: quality gates mark uncertainty when industry missing
- Capability execute → profiles on Artifact Bus
- Sales Intelligence derives richer pains/angles from BI
- Review payload includes BI before SI

## Acceptance Criteria

- [x] Company Intelligence (reasoning surface) replaced by Business Intelligence
- [x] Profiles emphasize reasoning rather than description
- [x] Sales Intelligence consumes Business Intelligence
- [x] Outputs expose confidence and uncertainty
- [x] Review Workspace presents Business Intelligence first
- [x] Messaging traces back to business reasoning (SI + mail attach BI / SI provenance)

## Future Work

- Knowledge Graph–backed Level-1 collection
- Vertical-specific operational models beyond seed taxonomy
- Objection prediction depth for Level 5
- Operator edit → re-reason BI revision on Mission Memory

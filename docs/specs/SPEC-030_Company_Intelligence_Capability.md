# SPEC-030 — Company Intelligence Capability

| Field | Value |
|---|---|
| **Status** | Proposed |
| **Target Version** | v1.2.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v0.1.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-028 (Client Playbook), SPEC-014 (Knowledge dual-write), ADR-002, ADR-005, ADR-010, ADR-011, ADR-017 |
| **Deepened by** | [SPEC-031](SPEC-031_Business_Signals_Capability.md) Business Signals (lifecycle, decay, Active set) / [ADR-018](../adr/ADR-018_Time_Matters.md) |
| **Supersedes (scope)** | Unfinished Company Enrichment work formerly tracked as SPEC-025 |
| **Consumed by** | Opportunity Ranking (SPEC-026), Campaign Builder, Proposal Generator (SPEC-027B), Execution Engine (SPEC-029), Knowledge Update, Command Deck Company Intelligence |

## Objective

Transform a discovered company into a **complete Company Intelligence Package** that powers Ranking, Campaign Builder, Proposal Generator, and Execution.

Every prospect that leaves Discovery receives an intelligence package. Every recommendation in that package traces to evidence. Nothing is invented. The Opportunity Brief inside the package becomes the canonical operator briefing for that company.

If Ranking scores a buying signal, Campaign Builder drafts a hook, Proposal Generator personalizes a section, or Execution dials a contact — each must already be grounded in this package. Downstream capabilities **consume the package without modification**.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Intelligence_Architecture.md`
- [ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md) — intelligence before execution; never fabricate
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable recommendations
- [ADR-005](../adr/ADR-005_LLM_Presentation_Engine.md) — presentation never invents intelligence
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine orchestration
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-014](../adr/ADR-014_Personalized_by_Default.md) — personalization requires real observations
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — Playbook frames *how*; intelligence frames *what we know*
- [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) — Execution must not invent contact/context
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md) — Discovery Profile + verified companies
- [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md) — consumes enrichment / intelligence fields
- [SPEC-027B](SPEC-027B_Proposal_Generator_Capability.md)
- [SPEC-028](SPEC-028_Client_Playbook_Capability.md)
- [SPEC-029](SPEC-029_Execution_Engine.md)

## Problem

Discovery produces verified companies. Downstream capabilities already *expect* enrichment signals (contacts, firmographics, buying signals, personalization angles) — see SPEC-026 factor scoring — but the live step is still a **stub** that fabricates placeholder emails and phones (`contactN@example.com`, `555-010N`). That violates explainability and makes Ranking / Campaign / Proposal / Execution look smarter than the evidence.

Contact backfill alone (classic “enrichment”) is not enough. Operators need a full intelligence package:

- Who runs the place (with confidence)
- What business signals are evidenced
- What to say first (evidence-backed hooks)
- Why they are a fit (Opportunity Brief)
- What Knowledge should store as fact vs inference

Without this capability **before Execution**, SPEC-029 either invents contact/context (forbidden by ADR-016) or executes blind campaigns that fail the Mission bar.

### Why this before Execution

Execution carries out approved strategy — it does not decide ([ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md)). Ranking, Campaign Builder, and Proposal Generator already score and draft from enrichment-shaped fields. Shipping Execution on the stub would lock in fabricated contacts and empty briefs. Company Intelligence closes the evidence gap so Execution has packages to carry out, not gaps to invent.

## Scope

- Company Intelligence capability replacing the SPEC-023 / SPEC-025 enrichment stub
- Registry id remains `company_enrichment` for MissionPlanner compatibility; operator-facing name becomes **Company Intelligence** / progress label **Building Company Intelligence**
- Inputs: Prospect (from Discovery), Discovery Profile, Client Playbook (when pinned)
- Intelligence categories (below) with confidence and evidence refs on every claim
- Company Intelligence Package per prospect (additive fields; existing consumers need no schema rewrite)
- Opportunity Brief generated automatically per package
- Knowledge Update handoff: verified facts → evidence; uncertain items → inferences only
- Progress stages: Collecting → Decision Makers → Signals → Personalizing → Briefing → Knowledge → Completed
- Deterministic v1 composition from evidence fields (no LLM inventing facts; optional later polish of verified prose only)
- Graceful degradation: missing evidence → empty arrays / stated uncertainty / lower confidence — **never** placeholder contacts

### Intelligence categories

#### 1. Company

Capture when evidenced:

| Field | Notes |
|---|---|
| Website | URL + verification status |
| Locations | Addresses / multi-location flags with sources |
| Industry | From Discovery + corroborating sources |
| Estimated size | Proxy only when sourced; else null + inference note |
| Years operating | Only when sourced |

#### 2. Decision Makers

Attempt to identify (never fabricate):

- Owner
- Office Manager
- Facilities Manager
- Property Manager
- Operations

Each candidate requires:

- `role` · `name?` · `email?` · `phone?` · `title?`
- `confidence` (0–1)
- `evidenceRefs[]`
- `status`: `verified` \| `inferred` \| `unknown`

Absent people stay absent. Empty decision-maker list + risk note is correct.

#### 3. Business Signals

Capture evidence for:

- Growth
- Hiring
- Expansion
- Multi-location
- Commercial footprint
- Recent changes

Each signal: `{ type, summary, confidence, evidenceRefs[], observedAt? }`. No signal without a ref.

**Deepening:** [SPEC-031](SPEC-031_Business_Signals_Capability.md) expands this category into a first-class time-aware subsystem (categories, lifecycle Detected→Archived, decay, Active set for Ranking/Brief/Campaign/Proposal, Knowledge objects). Company Intelligence’s Signals stage should call the SPEC-031 builders rather than inventing a parallel signal model. See [ADR-018](../adr/ADR-018_Time_Matters.md).

#### 4. Personalization

Generate **only** from evidenced company / signal / playbook fit:

- Relevant observations
- Conversation starters
- Outreach hooks

Every item carries `evidenceRefs`. Generic industry filler without a company-specific ref fails acceptance.

#### 5. Opportunity Brief

Produce the canonical operator briefing:

- Why they're a fit
- Suggested opening
- Talking points
- Risks
- Confidence

Aligns with SPEC-026 brief fields so Ranking can reuse or refine without inventing a second brief source of truth. When Ranking already produced a brief, Intelligence Brief is the **pre-rank** package brief; Ranking may refine scores but must not invent new facts.

#### 6. Knowledge Update

- Every **verified** fact becomes evidence (Knowledge Update / dual-write path)
- Everything **uncertain** remains an inference (explicitly typed; not promoted to fact)
- Capability emits structured `knowledgeWrites` for the Knowledge Update step — does not bypass SPEC-014

## Out of Scope

- Autonomous outreach / send
- Mutating Client Playbook or Discovery Profile
- LLM-authored business claims (ADR-005) — optional later polish of verified facts only
- Replacing Command Deck `CompanyIntelligenceComposer` UI model in v1 (composer continues to read Knowledge; packages feed Knowledge so UI improves without a parallel invent path)
- Full live Campaign Builder rewrite
- Execution Engine implementation (SPEC-029) — this spec unblocks it
- Auto-tuning hooks from win/loss (SPEC-021 later)

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-024 Discovery | Prospects + Discovery Profile + rankingSignals |
| SPEC-028 Client Playbook | Ideal customer / value props frame fit language (not fabricated company facts) |
| SPEC-023 Capability Framework | `company_enrichment` contract + progress events |
| SPEC-014 Knowledge dual-write | Persist verified evidence |
| SPEC-026 Ranking | Consumes package fields (`contacts`, `buyingSignals`, firmographics, `enriched`) without modification |
| Contact providers | Prospeo / Hunter / Places / website fetch — behind adapters; never invent on miss |
| ADR-017 | Never fabricate; ship before Execution |

## Architecture

```text
Prospect
Discovery Profile
Client Playbook (optional)
        ↓
Company Intelligence Capability
        ↓
┌───────────────────────────────────────┐
│ Company · Decision Makers · Signals   │
│ Personalization · Opportunity Brief   │
│ Knowledge writes (fact vs inference)  │
└───────────────────────────────────────┘
        ↓
Company Intelligence Package
        ↓
Opportunity Ranking
        ↓
Campaign Builder
        ↓
Proposal Generator
        ↓
Execution
```

### Design rules

1. **Never fabricate** — no placeholder emails, phones, titles, signals, or hooks ([ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md)).
2. **Evidence only** — every recommendation / observation carries `evidenceRefs`; absent evidence → omit or score 0 upstream.
3. **Fact vs inference** — verified facts update Knowledge as evidence; uncertain items stay typed inferences.
4. **Additive compatibility** — package includes fields Ranking already reads (`email`, `phone`, `website`, `contacts`, `buyingSignals`, `enriched`, firmographics). Existing consumers need no modification.
5. **Deterministic v1** — same inputs → same package (stable for tests and replay).
6. **Playbook frames fit language** — Playbook may shape *why fit* wording against ideal customer; it must not invent company attributes.

### Progress stages

| Stage | Meaning |
|---|---|
| Collecting | Firmographics / website / locations |
| Decision Makers | Role-targeted contact resolution |
| Signals | Business signal extraction |
| Personalizing | Observations / starters / hooks from evidence |
| Briefing | Opportunity Brief assembly |
| Knowledge | Emit fact / inference writes |
| Completed | Packages ready for Ranking |

### Mission flow (updated)

```text
Discovery → Company Intelligence → Knowledge Update → Opportunity Ranking → Campaign Builder → (Proposal) → Execution
```

Operator-facing copy uses “Company Intelligence”, not agent or provider names.

## Data Model

No requirement for a new durable table in v1 if packages live as mission capability artifacts. Optional persistence for operator reuse:

```text
packages/capabilities/intelligence/
  types.js
  firmographics.js
  decisionMakers.js
  signals.js
  personalize.js
  brief.js
  knowledgeHandoff.js
  CompanyIntelligence.js
  providers/          # contact / website / signal adapters
  index.js
```

### Package shape

```ts
interface CompanyIntelligencePackage {
  prospectId: string
  companyName: string
  company: {
    website: string | null
    locations: LocationFact[]
    industry: string | null
    estimatedSize: SizeFact | null
    yearsOperating: NumberFact | null
  }
  decisionMakers: DecisionMaker[]
  businessSignals: BusinessSignal[]
  personalization: {
    observations: PersonalizedItem[]
    conversationStarters: PersonalizedItem[]
    outreachHooks: PersonalizedItem[]
  }
  opportunityBrief: {
    whyFit: string
    suggestedOpening: string
    talkingPoints: string[]
    risks: string[]
    confidence: number // 0–1
    evidenceRefs: string[]
  }
  knowledgeWrites: KnowledgeWrite[]
  // Additive compatibility for SPEC-026 / stubs consumers:
  email: string | null
  phone: string | null
  website: string | null
  contacts: DecisionMaker[]
  buyingSignals: BusinessSignal[]
  enriched: boolean           // true only when at least one verified enrichment fact exists
  enrichmentConfidence: number
}

interface DecisionMaker {
  role: 'owner' | 'office_manager' | 'facilities_manager' | 'property_manager' | 'operations' | 'other'
  name: string | null
  title: string | null
  email: string | null
  phone: string | null
  confidence: number
  status: 'verified' | 'inferred' | 'unknown'
  evidenceRefs: string[]
}

interface PersonalizedItem {
  text: string
  evidenceRefs: string[]
  confidence: number
}

interface KnowledgeWrite {
  kind: 'evidence' | 'inference'
  subject: string
  claim: string
  confidence: number
  evidenceRefs: string[]
}
```

Capability `outputs`:

```ts
{
  prospects: Array<Prospect & { intelligencePackage: CompanyIntelligencePackage }>,
  packages: CompanyIntelligencePackage[],
  enrichedCount: number,       // count with enriched === true
  inferredOnlyCount: number,
  knowledgeWrites: KnowledgeWrite[]
}
```

## Implementation Plan

1. Spec + ADR-017 + types + empty-evidence fixtures (assert no fabrication)
2. Firmographics + decision-maker adapters (Prospeo/Hunter/website) with confidence gates
3. Business signal extractors (hiring / multi-location / commercial footprint) — evidence refs required
4. Personalization + Opportunity Brief builders (deterministic, Playbook-aware fit language)
5. Knowledge handoff emitter (`evidence` vs `inference`)
6. Replace enrichment stub registration; keep stub export for isolation tests
7. Capability tests: every prospect gets a package; no invented contacts; Ranking consumes unchanged
8. Update CURRENT_STATE / CHANGELOG / dependent specs that still say “SPEC-025 stub”

## Migration Strategy

- Additive capability package only; no forced CRM column rewrite in v1
- Mission plans already include `company_enrichment`; executor picks up live capability via registry
- Stub remains `createCompanyEnrichmentStub` for explicit tests — **production registry must not use fabricating stub**
- Rollback: re-register non-fabricating stub that passes through discovery fields with `enriched: false` (never invents contacts)
- Optional later: persist packages / briefs as mission artifacts in Postgres

## Testing

- Unit: each category with / without evidence
- Negative: providers miss → null contacts, empty signals, brief risks cite thin evidence — **zero** `@example.com` / fake phones
- Capability: Discovery fixture → Company Intelligence → Ranking (SPEC-026) without Ranking code changes
- Assert every prospect has `intelligencePackage` + Opportunity Brief
- Assert every personalization item and brief claim has `evidenceRefs` (or explicit uncertainty language)
- Assert `knowledgeWrites` separates `evidence` vs `inference`
- Assert `enriched === true` only when at least one verified fact exists
- Integration: `npm run test:capabilities` · `npm run test:mission`

## Acceptance Criteria

- [ ] Every prospect receives an intelligence package
- [ ] No invented facts (contacts, signals, hooks, firmographics)
- [ ] Every recommendation traces to evidence (`evidenceRefs` or stated uncertainty)
- [ ] Opportunity Brief generated automatically per package
- [ ] Knowledge updated with evidence; uncertain items remain inferences
- [ ] Existing capabilities consume the package without modification (Ranking / Campaign stub / Proposal path)
- [ ] Progress visible via capability progress events
- [ ] ADR-017 accepted and linked
- [ ] Fabricating enrichment stub removed from production registry path

## Future Work

- Richer website / GBP / hiring-board signal providers via [SPEC-031](SPEC-031_Business_Signals_Capability.md)
- Optional LLM polish that rephrases verified observations only (must not invent claims)
- Persist intelligence packages as first-class mission artifacts
- Align Command Deck Company Intelligence page exclusively on package + Knowledge (retire any dual invent path)
- SPEC-021 calibration of decision-maker / signal confidence from outcomes
- Narrow contact-only “enrichment retry” agents to call this capability instead of parallel invent paths

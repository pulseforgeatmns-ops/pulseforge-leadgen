# SPEC-027B — Proposal Generator Capability

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.2.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-030 (optional Company Intelligence), SPEC-026, SPEC-028 (Client Playbook), Campaign Builder (optional), ADR-010, ADR-011, ADR-014, ADR-015 |
| **Consumed by** | Mission Engine, Command Deck Operations, Max Workspace |

## Objective

Generate a premium, highly personalized sales proposal that reflects the discovery conversation, recommended strategy, and Pulseforge's evidence-driven approach.

The proposal is the **first deliverable the client receives**. It must demonstrate the quality of future work and make the prospect think: *"This was clearly written for my business."*

If another cleaning company received the same proposal with only the name changed, the Proposal Generator has failed ([ADR-014](../adr/ADR-014_Personalized_by_Default.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-014](../adr/ADR-014_Personalized_by_Default.md) — personalized by default
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-003](../adr/ADR-003_Human_Approval.md) — review before client delivery
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — evidence-backed recommendations
- [ADR-005](../adr/ADR-005_LLM_Presentation_Engine.md) — presentation never invents intelligence
- [SPEC-022](SPEC-022_Mission_Engine_and_Agent_Orchestration.md)
- [SPEC-023](SPEC-023_Capability_Framework.md)
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md)
- [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md)

## Problem

Pulseforge closes commercial-cleaning buyers (and similar clients) after discovery calls. Today there is no Mission capability that turns structured discovery into a reviewable, evidence-backed proposal. Hand-built HTML proposals in `proposals/` prove the bar for quality, but they do not scale and cannot learn from win/loss.

A template engine with name substitution would produce interchangeable decks and destroy the differentiator. The generator must be a **personalization engine**: every section references discovery facts, Discovery Profile strategy, and (when present) ranking / campaign evidence.

## Scope

- Proposal Generator capability (`proposal_generator`) as a first-class Mission capability
- Mission type `proposal_generation` routed from objectives like “Generate proposal for AS Cleaning”
- Inputs: Discovery Summary, Discovery Profile, optional Recommended Strategy (discovery + ranking + campaign), operator-selected Pricing Package
- Eleven-section proposal structure (Cover → Next Steps)
- Personalization engine: deterministic, evidence-backed prose composition (no placeholder text, no generic interchangeable paragraphs)
- Evidence rules: every recommendation traces to discovery notes / profile / opportunity / campaign; uncertainty stated when evidence is thin
- Operator review package with editable fields (pricing, timeline, strategy, recommendations, closing, notes)
- Output: interactive web proposal HTML + printable version; shareable artifact id
- Version store (in-memory + Postgres): proposal versions attached to opportunity / mission
- Learning-loop fields on versions: accepted changes, client outcome, win/loss, feedback (populated later by operators)

## Out of Scope

- Live PDF binary generation in the capability path (v1 returns printable HTML; Puppeteer export may wrap the HTML artifact separately)
- Client portal view (Future)
- Autonomous send to client without operator review
- LLM-authored business claims (ADR-005) — optional LLM polish may only rephrase verified facts in a later slice
- Full Campaign Builder live adapter (consumes stub or live campaign outputs when present)
- Auto-tuning copy from win/loss (SPEC-021 may inform later)

## Dependencies

- Capability Framework contract (SPEC-023)
- Discovery Profiles (SPEC-024) when strategy is profile-driven
- Opportunity Ranking / Campaign Builder outputs when mission context includes them (optional — proposal can generate from Discovery Summary + Profile alone)
- Mission Engine routing + durable missions

## Architecture

```text
Discovery Call
      ↓
Discovery Summary (structured)
Discovery Profile (selected)
Recommended Strategy (optional: Discovery · Ranking · Campaign)
Pricing Package (operator-selected)
      ↓
Proposal Generator Capability
      ↓
Personalization Engine (evidence-backed section composition)
      ↓
Operator Review (editable)
      ↓
Web / Printable Proposal · Version stored with opportunity
```

### Design rules

1. **Personalized by default (ADR-014)** — optimize for relevance, not speed. Name-swap alone must fail acceptance tests.
2. **Evidence only** — never invent challenges, goals, markets, or pricing rationale. When uncertain, say so.
3. **Not a template engine** — sections are composed from discovery facts; shared scaffolding is allowed, interchangeable body copy is not.
4. **Review-gated** — nothing is client-facing until operator approve / edit.
5. **Deterministic v1** — same inputs → same proposal document (stable for tests and replay).

### Proposal structure

| # | Section | Source |
|---|---|---|
| 1 | Cover | Company name, prepared by Pulseforge |
| 2 | Executive Summary | Discovery summary woven into consultative prose |
| 3 | Understanding Your Business | Current state, goals, challenges, growth vision |
| 4 | Why Pulseforge | Solution framed in the client's goals |
| 5 | Recommended Strategy | Discovery Profile markets + why selected |
| 6 | What We Handle | Dynamic scope from strategy |
| 7 | Your Role | Operator / client responsibilities |
| 8 | First 90 Days | Month 1 Foundation · Month 2 Optimization · Month 3 Scale |
| 9 | Long-Term Advantage | Approved messaging block + evidence learning |
| 10 | Investment | Operator pricing package + inclusions / schedule / terms |
| 11 | Next Steps | Approval approval → kickoff → profile approval → campaign launch |

### Tone

Professional · Consultative · Confident. Never hype. Never generic. Never AI-sounding.

## Data Model

```text
packages/capabilities/proposal/
  types.js
  pricing.js
  evidence.js
  personalize.js
  render.js
  ProposalStore.js
  PostgresProposalStore.js
  ProposalGenerator.js
  index.js

migrations/2026-07-27-proposal-generator.sql
```

### Discovery Summary (input)

```ts
interface DiscoverySummary {
  companyName: string
  contactName?: string
  industry?: string
  geography?: string
  companyStage?: string
  currentClients?: string | string[]
  revenue?: string | null
  currentMarketingChannels?: string | string[]
  icp?: string | string[]
  currentProcess?: string
  challenges?: string | string[]
  goals?: string | string[]
  growthVision?: string
  notes?: string
}
```

### Proposal version

```ts
interface ProposalVersion {
  id: string
  opportunityId?: string | null
  missionId?: string | null
  clientId?: string | number | null
  tenantId: string
  version: number
  status: 'draft' | 'review' | 'approved' | 'sent' | 'won' | 'lost'
  discoverySummary: DiscoverySummary
  discoveryProfileId?: string | null
  pricingPackageId: string
  document: ProposalDocument
  acceptedChanges?: object[]
  clientOutcome?: string | null
  winLoss?: 'win' | 'loss' | null
  feedback?: string | null
  createdAt: string
  updatedAt: string
}
```

## Implementation Plan

1. Spec + ADR-014 + types + pricing packages + evidence helpers
2. Personalization engine + HTML renderer
3. `createProposalGeneratorCapability` with progress events + review package + version store
4. Mission type `proposal_generation` + IntentRouter patterns
5. Postgres migration + in-memory store
6. Capability + mission tests
7. Update CURRENT_STATE / CHANGELOG / indexes

## Migration Strategy

- Forward: `migrations/2026-07-27-proposal-generator.sql` (`proposal_versions`)
- Rollback: companion `.rollback.sql`
- Capability works with in-memory store when Postgres is unavailable (tests / local)

## Testing

- Unit: personalization references company-specific facts; name-swap fixture fails interchangeability check
- Unit: missing evidence → uncertainty language, never invented markets
- Capability: full document from Discovery Summary + Profile + pricing
- Capability: operator review actions present; no client send side effects
- Mission: “Generate proposal for …” → `proposal_generation` chain

## Acceptance Criteria

- [x] Generates a complete proposal from structured discovery data
- [x] References client-specific details throughout (interchangeability test)
- [x] Supports editable pricing packages (operator-selected)
- [x] Produces web-ready + printable HTML output (PDF export deferred to wrapper)
- [x] No placeholder text (`TODO`, `TBD`, `[insert]`, lorem)
- [x] No generic interchangeable body paragraphs under acceptance tests
- [x] Every recommendation carries evidence refs
- [x] Proposal is reviewable before delivery
- [x] Proposal versions are stored with the opportunity / mission

## Future Work

- Puppeteer PDF export hooked to approved versions
- Shareable signed client link + client portal view
- Optional LLM polish pass that may only rephrase verified facts (ADR-005)
- Win/loss feedback loop into SPEC-021 calibration of messaging blocks
- Deeper Campaign Builder strategy narrative when live adapter ships

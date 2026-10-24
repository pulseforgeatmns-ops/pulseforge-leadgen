# SPEC-031 — Business Signals Capability

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.2.2 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-030 (Company Intelligence), SPEC-023, SPEC-014, SPEC-001A/C (Knowledge), SPEC-003 (Temporal Memory — decay alignment), ADR-002, ADR-005, ADR-011, ADR-017, ADR-018 |
| **Consumed by** | Opportunity Ranking (SPEC-026), Opportunity Briefs, Campaign Builder, Proposal Generator (SPEC-027B), Knowledge Update, Command Deck / prospect workspace |

## Objective

Teach Pulseforge to recognize when a business is likely to be receptive **before outreach ever begins**.

Finding companies is not enough. Understanding what is happening inside the company creates timing advantages.

Transform public evidence into actionable **Business Signals**. Every signal must answer:

> Why might this company be worth contacting **right now**?

If SPEC-030 teaches Pulseforge *who* a company is, SPEC-031 teaches it *what the company is doing right now*. That is the difference between:

> This is a property management company.

and

> This property management company just added 12 new listings and opened a satellite office.

The second one changes how you sell.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- `docs/vision/Intelligence_Architecture.md`
- [ADR-018](../adr/ADR-018_Time_Matters.md) — time-aware intelligence; signals decay
- [ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md) — never fabricate; intelligence before execution
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable recommendations
- [ADR-005](../adr/ADR-005_LLM_Presentation_Engine.md) — presentation never invents intelligence
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [SPEC-030](SPEC-030_Company_Intelligence_Capability.md) — Company Intelligence Package (signals category deepened here)
- [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md) — Buying Signals / timing factors
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md) — temporal change as first-class intelligence
- [SPEC-014](SPEC-014_Knowledge_Dual_Write.md) — evidence dual-write
- [SPEC-027B](SPEC-027B_Proposal_Generator_Capability.md)

## Problem

Company Intelligence (SPEC-030) sketches a `businessSignals[]` field, and Ranking (SPEC-026) already scores a Buying Signals factor — but today those are thin stubs or enrichment flags (`hiringActivity`, ad-hoc keys). There is no:

- Evidence-backed signal model with confidence and source
- Verification step before a signal becomes Active
- Temporal lifecycle (detect → verify → active → decay → archive)
- Category taxonomy (growth / operational / marketing / organizational / buying)
- Operator surface that shows Active Business Signals with confidence
- Contract that Ranking, Campaign Builder, Opportunity Briefs, and Proposal Generator consume the same active-signal set

Without this, “timing” remains speculative. Opportunity Briefs stay generic (“Good fit.”) instead of citing what changed.

## Design Principles

Business Signals are:

1. **Evidence-backed** — no signal without `evidence` / `evidenceRefs`
2. **Time-aware** — `observedAt`, optional `expiresAt`, decay over time ([ADR-018](../adr/ADR-018_Time_Matters.md))
3. **Explainable** — title + description answer “why contact now?”
4. **Confidence scored** — High / Medium / Low / Unknown
5. **Never speculative** — observations only, not conclusions about intent to buy

Signals represent **observations — not conclusions**.

## Scope

- Business Signals subsystem (library + capability hook) deepening SPEC-030’s Signals stage
- Signal collection adapters (public sources only; never invent)
- Evidence verification gate before Active status
- Signal model + lifecycle + decay
- Five signal categories (below)
- Knowledge handoff: signals as first-class Knowledge objects with evidence attached
- Consumption contracts for Ranking, Opportunity Brief, Campaign Builder, Proposal Generator
- Operator UX contract: Active Business Signals + confidence on each prospect
- Deterministic v1 composition from evidenced fields (no LLM inventing signals)
- Graceful degradation: no evidence → empty active set — **never** fabricated observations

### Signal categories

#### Growth Signals

Examples: hiring · new locations · office expansion · service expansion · team growth

#### Operational Signals

Examples: commercial office footprint · multiple locations · franchise · property portfolio · regulated environments

#### Marketing Signals

Examples: new website · recent rebrand · active LinkedIn · new Google Business photos · increased review activity

#### Organizational Signals

Examples: leadership changes · ownership changes · office relocation · new partnerships

#### Buying Signals

Examples: new lease · office renovation · property acquisition · hiring facilities personnel · expansion announcements

### Capability flow

```text
Company
      ↓
Signal Collection
      ↓
Evidence Verification
      ↓
Business Signals (Active)
      ↓
Opportunity Ranking
      ↓
Opportunity Brief
```

Within Company Intelligence progress stages, this is the **Signals** stage. Ranking and Briefs consume the Active set; Campaign Builder and Proposal Generator reference the same Active set when present.

## Out of Scope

- Fabricating or inferring purchase intent (“they will buy cleaning”)
- Autonomous outreach triggered solely by a signal
- Live Campaign Builder rewrite (SPEC-031 defines the signal contract; Campaign Builder adapters land separately)
- Replacing Ranking factor weights wholesale (Ranking consumes Active signals; weight tuning may follow SPEC-021)
- Scraping that violates source ToS or invents private company data
- LLM-authored signal claims (ADR-005) — optional later polish of verified observations only

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-030 Company Intelligence | Hosts Signals stage; package field `businessSignals` / `buyingSignals` filled by this subsystem |
| SPEC-023 Capability Framework | Progress events; registry compatibility |
| SPEC-014 / Knowledge | Persist verified signals as Knowledge objects + evidence |
| SPEC-026 Ranking | Buying Signals / timing / priority / outreach angle consume Active signals |
| SPEC-003 Temporal Memory | Decay and archival align with “change over time” posture |
| ADR-018 | Time matters — recent verified signals increase relevance; expired signals lose influence |
| ADR-017 | Never fabricate |

## Architecture

```text
Company (+ Discovery / Intelligence inputs)
        ↓
Signal Collection (adapters: Places, website, GBP, hiring boards, public announcements, …)
        ↓
Evidence Verification (corroboration + confidence gate)
        ↓
BusinessSignal records (lifecycle)
        ↓
┌─────────────────────────────────────────────┐
│ Knowledge (first-class signal + evidence)   │
│ Company Intelligence Package.businessSignals│
│ Ranking · Brief · Campaign · Proposal       │
│ Operator: Active Business Signals UI        │
└─────────────────────────────────────────────┘
```

### Design rules

1. **Never fabricate** — absent evidence → no signal ([ADR-017](../adr/ADR-017_Intelligence_Before_Execution.md)).
2. **Observations only** — titles/descriptions state what was observed, not what the company “wants.”
3. **Verification before Active** — Detected signals are not ranked until Verified (or explicitly Low/Unknown and still non-Active if policy requires).
4. **Decay is mandatory** — Active signals lose influence after `expiresAt` or category default TTL ([ADR-018](../adr/ADR-018_Time_Matters.md)).
5. **One Active set** — Ranking, Brief, Campaign, Proposal, and operator UI all read the same Active (non-decayed) signals.
6. **Explainable confidence** — High / Medium / Low / Unknown with documented meaning.
7. **Deterministic v1** — same inputs → same signals (stable for tests and replay).

### Signal lifecycle

```text
Detected → Verified → Active → Decays → Archived
```

| State | Meaning |
|---|---|
| Detected | Collector emitted a candidate with at least one evidence ref |
| Verified | Passed corroboration / confidence gate for its category |
| Active | Eligible for Ranking, Briefs, Campaign, Proposal, operator UI |
| Decays | Past soft TTL or approaching `expiresAt`; influence weight reduced |
| Archived | Expired or superseded; retained for audit / Memory, not ranking |

Old signals naturally lose influence over time. Archived signals remain queryable for explainability (“we saw expansion in March; it no longer affects priority”).

### Confidence

| Level | Meaning |
|---|---|
| **High** | Official announcement or primary source with clear attribution |
| **Medium** | Multiple corroborating sources |
| **Low** | Single indirect observation |
| **Unknown** | Insufficient evidence — must not become Active for ranking influence |

Numeric mapping for Ranking (0–1): High ≥ 0.85 · Medium ≥ 0.6 · Low ≥ 0.35 · Unknown = 0 (excluded from Active influence).

### Downstream integration

#### Opportunity Brief

Rather than:

> Good fit.

Pulseforge says:

> Recently expanded into a second office and appears to be hiring administrative staff.

Brief builders MUST prefer Active signal titles/descriptions over generic fit language when signals exist.

#### Opportunity Ranking

Active signals influence (all weights remain explainable):

| Concern | How signals apply |
|---|---|
| Buying Signals factor | Active `buying` (+ related growth/ops) signals with evidence refs |
| Timing | Recency + confidence; decaying signals contribute less |
| Priority | Soft boost when High/Medium Active buying or expansion signals exist |
| Outreach angle | Top Active signal type selects recommended angle language |

#### Campaign Builder

Messaging keyed off Active signal types (contract only in v1):

| Signal type family | Messaging posture |
|---|---|
| Expansion / new location | Growth messaging |
| Hiring | Operational efficiency messaging |
| Property acquisition / new lease / renovation | Recurring maintenance messaging |

Campaign Builder must not invent a type that is not Active.

#### Proposal Generator

May explain market selection using aggregated Active signals across the ranked set, e.g.:

> We selected these markets because many businesses currently show signs of growth and recurring facility demand.

Every such claim must cite signal evidence aggregates — no fabricated market narrative.

#### Knowledge

Signals become first-class Knowledge objects. Evidence remains attached. Nothing is inferred without support. Uncertain candidates stay Detected/Unknown and are written only as `inference` KnowledgeWrites (or omitted), never as facts.

### Operator experience

Each prospect displays **Active Business Signals**, for example:

- 🟢 Hiring Office Staff (High)
- 🟢 Recently Expanded (Medium)
- 🟡 New Website (Low)

Confidence shown beside every signal. Color maps to confidence band (not “buy likelihood”). Empty Active set is valid and preferred over placeholders.

## Data Model

```text
packages/capabilities/signals/
  types.js
  categories.js
  lifecycle.js          # Detected → … → Archived + decay weights
  confidence.js
  collect.js            # adapter orchestration
  verify.js
  decay.js
  knowledgeHandoff.js
  BusinessSignals.js    # capability / package builder entry
  providers/            # hiring, places, website, gbp, announcements, …
  index.js
```

Company Intelligence (SPEC-030) calls into this package during the Signals stage and sets:

- `intelligencePackage.businessSignals`
- `intelligencePackage.buyingSignals` (Active subset filtered to buying category + high-timing growth/ops as Ranking already expects)

### Signal shape

```ts
type SignalConfidence = 'high' | 'medium' | 'low' | 'unknown'
type SignalCategory =
  | 'growth'
  | 'operational'
  | 'marketing'
  | 'organizational'
  | 'buying'
type SignalLifecycle =
  | 'detected'
  | 'verified'
  | 'active'
  | 'decaying'
  | 'archived'

interface BusinessSignal {
  id: string
  type: string                 // e.g. hiring_office_staff, new_location, new_website
  category: SignalCategory
  title: string                // operator-facing short label
  description: string          // answers “why contact now?”
  confidence: SignalConfidence
  confidenceScore: number      // 0–1 mapping
  lifecycle: SignalLifecycle
  observedAt: string           // ISO
  source: string               // adapter / provenance label
  evidence: SignalEvidence[]
  evidenceRefs: string[]
  expiresAt?: string           // ISO; category default if omitted at verify time
  influenceWeight: number      // 0–1 after decay; 0 when archived
  companyId?: string
  prospectId?: string
}

interface SignalEvidence {
  kind: string
  summary: string
  url?: string
  observedAt?: string
  rawRef?: string
}
```

### Capability / package outputs

When invoked from Company Intelligence (or standalone for backfill):

```ts
{
  signals: BusinessSignal[],
  activeSignals: BusinessSignal[],      // lifecycle active|decaying with influenceWeight > 0
  buyingSignals: BusinessSignal[],      // Active buying (+ timing-relevant) for SPEC-026
  archivedCount: number,
  knowledgeWrites: KnowledgeWrite[]     // evidence for verified; inference for uncertain
}
```

### Default TTLs (v1 starting point)

| Category | Soft decay starts | Hard expire (default) |
|---|---|---|
| Buying | 30 days | 90 days |
| Growth | 45 days | 120 days |
| Organizational | 60 days | 180 days |
| Operational | 90 days | 365 days (footprint often sticky) |
| Marketing | 30 days | 90 days |

TTL overrides allowed per signal when the source states an explicit end date. Decay function is deterministic: `influenceWeight = confidenceScore * max(0, 1 - age/TTL)` while Active/Decaying.

## Implementation Plan

1. Spec + ADR-018 + types + empty-evidence fixtures (assert no fabrication)
2. Lifecycle + confidence + decay helpers (unit-tested, deterministic)
3. Collectors for v1: multi-location / commercial footprint (Places), hiring (public boards / site careers page when present), website freshness / rebrand heuristics (only with evidence)
4. Verification gate: High/Medium → Active eligible; Low → Active only if Playbook marks preferred; Unknown never Active
5. Wire into SPEC-030 Signals stage + `buyingSignals` additive field for Ranking
6. Ranking factor update: prefer `activeSignals` / structured `BusinessSignal` over boolean `hiringActivity` when present (backward compatible)
7. Brief builder: inject Active signal prose when present
8. Knowledge handoff emitter + operator Active Signals view model
9. Campaign Builder / Proposal contracts documented; stub adapters may reference Active types without inventing
10. Tests + CURRENT_STATE / CHANGELOG / dependent spec cross-links

## Migration Strategy

- Additive: no CRM column rewrite required in v1; signals live on intelligence packages / mission artifacts
- Optional later: durable `business_signals` table or Knowledge node type for cross-mission reuse
- Ranking continues to accept legacy enrichment flags when structured signals are absent
- Rollback: Company Intelligence emits empty `businessSignals` / `buyingSignals` — Ranking scores 0 on buying factor (honest), never invents

## Testing

- Unit: each category with / without evidence; lifecycle transitions; decay weights
- Negative: collectors miss → empty Active set — **zero** invented hiring/expansion claims
- Assert every Active signal has evidence + observedAt + confidence
- Assert Unknown never contributes to Ranking influence
- Assert expired signals are Archived and excluded from operator Active list
- Capability: Discovery fixture → Company Intelligence (Signals) → Ranking without inventing
- Brief: with Active expansion+hiring → brief cites them; without → uncertainty / generic fit only
- Integration: `npm run test:capabilities` · `npm run test:mission`

## Acceptance Criteria

- [x] Signals are evidence-backed (every Active signal has evidence / evidenceRefs)
- [x] Signals decay over time (TTL + influenceWeight; Archived excluded from Active)
- [x] No fabricated observations
- [x] Opportunity Ranking consumes Active signals (Buying Signals / timing / angle)
- [x] Campaign Builder references Active signal types for messaging posture (contract + adapter hook)
- [x] Opportunity Briefs include Active signals when present
- [x] Signals remain explainable and traceable (source, evidence, confidence, lifecycle)
- [x] Knowledge receives verified signals as evidence; uncertain items stay inferences
- [ ] Operator surface can list Active Business Signals with confidence (operatorSignals view model ready; Command Deck UI bind later)
- [x] ADR-018 accepted and linked

## Future Work

- Richer providers: GBP photo deltas, review velocity, LinkedIn org posts, lease/permit public records
- Command Deck Company Intelligence page section bound exclusively to Active + Archived signals
- SPEC-021 calibration of signal confidence and TTLs from win/loss outcomes
- Optional LLM polish that rephrases verified observations only (must not invent claims)
- Durable cross-mission signal store / Knowledge node type `BusinessSignal`
- Real-time signal watchers (pair with SPEC-003 watches) for “what changed this week”

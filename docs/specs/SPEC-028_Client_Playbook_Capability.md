# SPEC-028 — Client Playbook Capability

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v1.2.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-024, SPEC-026, SPEC-027B, ADR-010, ADR-011, ADR-014, ADR-015 |
| **Consumed by** | Mission Engine, Campaign Builder, Proposal Generator, Command Deck (future editor) |

## Objective

A **Client Playbook** captures how a specific business wins customers.

- Discovery Profiles answer: *Who should we target?*
- Client Playbooks answer: *How should we sell to them?*

Every campaign, proposal, and future execution references the Client Playbook before making recommendations. Playbooks are strategic assets — versioned, evidence-driven, client-specific, operator-editable, reusable across missions, and explainable ([ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — strategy lives in the Playbook
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities execute strategy
- [ADR-014](../adr/ADR-014_Personalized_by_Default.md) — proposals consume playbook language
- [ADR-002](../adr/ADR-002_Explainable_AI.md) — explainable decisions
- [ADR-003](../adr/ADR-003_Human_Approval.md) — operator edits / learning recommendations require approval
- [SPEC-024](SPEC-024_Prospect_Discovery_Capability.md) — Discovery Profiles (who)
- [SPEC-026](SPEC-026_Opportunity_Ranking_Capability.md)
- [SPEC-027B](SPEC-027B_Proposal_Generator_Capability.md)

## Problem

Downstream capabilities currently invent or hardcode outreach assumptions (channel order, sequence timing, offers, brand voice). Strategy is scattered across stubs and prompts instead of a single client-owned asset. Without a pinned Playbook version, historical missions cannot explain *why* they sold the way they did, and Proposal Generator / Campaign Builder drift apart.

## Scope

- Client Playbook as a first-class versioned asset (in-memory + Postgres)
- Playbook model sections: Ideal Customer, Value Proposition, Brand Voice, Preferred Channels, Outreach Sequence, Offers, Constraints, Success Metrics, Operator Notes
- Immutable versioning (edits create a new version; missions pin the version used)
- `PlaybookSelector` for MissionPlanner (explicit pin → client match → seed default)
- Campaign Builder consumes: preferred channels, outreach sequence, constraints, offers, brand voice
- Proposal Generator consumes: brand voice, value propositions, offers, ideal customer, success metrics
- Seed playbooks (AS Cleaning Co., Anchor Cleaning)
- No duplicate strategy configuration in downstream capabilities

## Understanding vs strategy

- **Business Blueprint (SPEC-083)** = understanding — *who is this business?*
- **Client Playbook (this spec)** = strategy — *how should Pulseforge help grow this business?*

CIE ends at an approved Blueprint. On approval it may generate a `pending_review` playbook from understanding fields only; operators still review and activate. Scout, Composer, and campaigns continue to consume playbooks — never blueprints directly.

## Out of Scope

- Visual Playbook editor UI (Future)
- Automated learning-loop recommendations that mutate playbooks (advisory only later; operator approval always required)
- Full live Campaign Builder (still stub adapter; stub now respects Playbook)
- Replacing Discovery Profiles (orthogonal: who vs how)

## Dependencies

- Capability Framework (SPEC-023)
- Mission Engine routing + durable constraints snapshots (SPEC-022)
- Discovery Profiles (SPEC-024) remain the “who” asset
- Proposal Generator (SPEC-027B) personalization engine
- Campaign Builder capability slot (stub → live later)

## Architecture

```text
Client
      ↓
Business Blueprint (understanding — SPEC-083)
      ↓
Client Playbook  (strategy — ADR-015)
      ↓
Campaign Builder  ← channels · sequence · constraints · offers
      ↓
Proposal Generator ← voice · value props · offers · ICP · metrics
      ↓
Execution Engine (future)
```

### Design principles

1. **Strategy in the Playbook** — capabilities execute; they do not own client strategy.
2. **Versioned** — immutable once used; updates bump version.
3. **Explainable** — missions store `clientPlaybookId` + `clientPlaybookVersion` (+ snapshot).
4. **Operator-editable** — structured fields + free-form notes.
5. **Reusable** — one active playbook per client informs all mission types that sell.

### Mission flow

```text
Client → Client Playbook → Campaign Builder → Proposal Generator → Execution
```

Discovery Profiles continue to drive *who* to find; Playbooks drive *how* to engage and sell.

## Data Model

```text
packages/capabilities/playbook/
  types.js
  seedPlaybooks.js
  ClientPlaybookStore.js
  PostgresClientPlaybookStore.js
  PlaybookSelector.js
  apply.js
  index.js

migrations/2026-07-27-client-playbooks.sql
```

```ts
interface ClientPlaybook {
  id: string
  clientId: number | string
  name: string
  version: string
  status: 'active' | 'pending_review' | 'superseded' | 'draft'
  targetMarkets: string[]
  valuePropositions: string[]
  idealCustomer: IdealCustomer
  brandVoice: BrandVoice
  preferredChannels: string[]          // ranked
  outreachSequence: OutreachStep[]
  offers: string[]
  constraints: PlaybookConstraint[]
  successMetrics: string[]
  notes: string
  parentId: string | null
  createdAt: string
  updatedAt: string
}

interface IdealCustomer {
  primaryMarkets: string[]
  secondaryMarkets: string[]
  geographicCoverage: string
  minimumCompanySize: string | null
  industriesToAvoid: string[]
  buyingTriggers: string[]
}

type BrandVoice =
  | 'professional'
  | 'friendly'
  | 'relationship_first'
  | 'technical'
  | 'premium'
  | 'direct'

interface OutreachStep {
  day: number
  channel: string
  action: string
  notes?: string
}

interface PlaybookConstraint {
  type: string   // e.g. call_window | exclude_industry | focus | exclude_crm
  rule: string
  detail?: string
}
```

## Implementation Plan

1. Types + seed playbooks + in-memory store (versioning / approve)
2. Postgres store + migration
3. PlaybookSelector + MissionPlanner pin into constraints
4. Campaign Builder stub reads Playbook (no hardcoded outreach)
5. Proposal Generator personalization consumes Playbook sections
6. Tests + CURRENT_STATE / CHANGELOG / DECISIONS / ADR-015

## Migration Strategy

- Additive table `client_playbooks` `(id, version)` PK
- Idempotent `ENSURE_SQL` in Postgres store for local/dev
- Rollback SQL drops table only
- Historical mission rows retain JSON snapshots in `missions.constraints`

## Testing

- Unit: build / version / approve / seed AS Cleaning + Anchor
- Campaign Builder: sequence + channels come from playbook; constraints applied
- Proposal Generator: value props / offers / voice appear with evidence refs
- MissionPlanner: campaign + proposal missions pin playbook version
- `npm run test:capabilities` · `npm run test:mission`

## Acceptance Criteria

- [x] Client Playbooks are first-class versioned assets
- [x] Every Campaign references a Playbook (pinned on mission constraints)
- [x] Proposal Generator consumes Playbook content
- [x] Campaign Builder respects Playbook preferences
- [x] Historical missions remain pinned to the Playbook version used
- [x] No hardcoded outreach assumptions remain in Campaign Builder stub when a Playbook is present
- [x] ADR-015 accepted and linked

## Future Work

- CIE consumes an approved Business Blueprint and maps business understanding into a draft Client Playbook ([SPEC-083](SPEC-083_Client_Intelligence_Engine.md)) — v1 handoff shipped; richer strategy-field operator tooling remains
- Visual Playbook editor
- Advisory learning recommendations (“Medical offices convert 38% better…”) requiring operator approval
- Execution Engine enforcement of call windows / CRM exclusions at send time ([SPEC-029](SPEC-029_Execution_Engine.md) / [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md))
- Multi-playbook per client (segment-specific) with explicit mission selection

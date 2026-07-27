# SPEC-033 — Mail Package Generator

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-028 (Client Playbook), Campaign Builder outputs, SPEC-026 (opportunity briefs), SPEC-029 (Execution consumes packages), SPEC-030 (Company Intelligence — optional), SPEC-032 (Mission Memory — revision pin), ADR-003, ADR-010, ADR-011, ADR-014, ADR-015, ADR-016 |
| **Consumed by** | Mission Engine, Command Deck Operations, Max Workspace, Execution Engine (Direct Mail channel) |

## Objective

Produce a complete, print-ready direct mail package for every **approved** campaign prospect.

Operators must be able to print letters, stuff envelopes from a checklist, and export mail-merge / address-label CSVs — without inventing strategy (ADR-015 / ADR-016). Personalization must be evidence-backed (ADR-014). Missing address, company, recipient (with company fallback), or low personalization confidence **blocks Ready-to-Print**.

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-003](../adr/ADR-003_Human_Approval.md) — review before print / ship
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-014](../adr/ADR-014_Personalized_by_Default.md) — personalized by default
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — voice / offers / inserts from playbook
- [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) — Execution consumes packages; does not invent letters
- [SPEC-028](SPEC-028_Client_Playbook_Capability.md)
- [SPEC-029](SPEC-029_Execution_Engine.md) — Direct Mail = manual packet assembly + ship confirmation
- [SPEC-030](SPEC-030_Company_Intelligence_Capability.md)
- [SPEC-032](SPEC-032_Mission_Memory.md)

## Problem

Campaign Builder drafts `mailMerge` openers and ranks prospects, but there is no capability that turns an approved prospect list into:

1. Personalized letters + envelopes
2. Operator personalization summaries and insert checklists
3. Validated Ready-to-Print vs Needs Review status
4. Campaign PDF / mail-merge CSV / address-label CSV exports
5. Revision history when letters are regenerated

Without Mail Package Generator, Direct Mail execution (SPEC-029) has nothing durable to assemble.

## Scope

- Mail Package Generator capability (`mail_package_generator`) as a first-class Mission capability
- Mission type `mail_package_generation` (and optional post-campaign invocation with approved campaign inputs)
- Inputs: Approved Mission context, Approved Prospect List, Company Intelligence Package (when present), Client Playbook, Campaign Strategy
- Per-prospect outputs:
  - Personalized Letter (recipient, company, opening, value prop, CTA, signature)
  - Envelope (recipient, company, mailing address, return address)
  - Personalization Summary (why selected, facts used, letter confidence, missing-data warnings)
  - Insert Checklist (Letter, Business Card, Brochure, Microfiber Cloth, Coupon, Handwritten Note — configurable)
- Campaign summary: Prospects · Ready to Print · Needs Review · Missing Addresses · Estimated Print Time · Estimated Assembly Time
- Validation gates before Ready-to-Print
- Operator review actions: edit letter, regenerate letter, skip prospect, mark address invalid, replace recipient, approve package
- Exports: printable HTML (individual + combined campaign), Mail Merge CSV, Address Label CSV, Word-compatible DOCX HTML
- Version store (in-memory + optional Postgres later): regeneration appends a revision; prior revisions remain available
- Artifacts shaped for Mission Memory `activeArtifacts` when SPEC-032 lands

## Out of Scope

- Live Puppeteer PDF binary inside the capability hot path (v1 returns printable HTML; Puppeteer may wrap HTML like SPEC-027B)
- Autonomous postage purchase / carrier API
- Handwriting simulation / physical print hardware control
- Inventing mailing addresses or decision-makers when Company Intelligence is missing (fail to Needs Review)
- Execution Engine shipping confirmation (SPEC-029)
- Full Mission Memory tables (SPEC-032) — local revision store mirrors the contract

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-023 Capability Framework | Capability contract + registry |
| SPEC-028 Client Playbook | Brand voice, value props, offers, preferred channels, return identity |
| Campaign Builder | Approved prospect list + campaign strategy + mailMerge seeds |
| SPEC-026 Opportunity Briefs | Why selected / talking points when present |
| SPEC-030 Company Intelligence | Decision-makers, contacts, address enrichment (optional) |
| SPEC-032 Mission Memory | Pin package artifacts on `activeArtifacts`; append revision on regenerate |
| SPEC-029 Execution Engine | Consumes Ready-to-Print packages for Direct Mail tasks |

## Architecture

```text
Approved Campaign / Mission
      ↓
Approved Prospect List
Client Playbook · Campaign Strategy
Company Intelligence (optional) · Opportunity Briefs
      ↓
Mail Package Generator
      ↓
Validate (address · company · recipient · confidence)
      ↓
Per prospect: Letter · Envelope · Summary · Insert Checklist
      ↓
Campaign Summary + Exports (HTML · CSV · DOCX HTML)
      ↓
Operator Review → Ready to Print | Needs Review
      ↓
Mission Memory artifact / Execution Direct Mail (SPEC-029)
```

### Design rules

1. **Fail closed** — missing mailing address, company, or recipient (after company fallback) → Needs Review; never Ready to Print.
2. **Personalized by default (ADR-014)** — letter body references prospect + playbook evidence; placeholder / template-only letters fail validation.
3. **Strategy from Playbook (ADR-015)** — voice, value propositions, CTA/offers, default insert kit from playbook / campaign strategy.
4. **Review-gated (ADR-003)** — package approval is explicit; print status is separate from mission approval.
5. **Execution does not decide (ADR-016)** — packages are artifacts Execution consumes.
6. **Deterministic v1** — same inputs → same letter / CSV rows (stable for tests and replay).

### Validation (before Ready to Print)

| Check | Failure |
|---|---|
| Mailing address exists | Needs Review · Missing Addresses |
| Company name exists | Needs Review |
| Recipient exists **or** company fallback | Needs Review if neither |
| Personalization confidence ≥ threshold (default `0.65`) | Needs Review |

### Review actions

| Action | Effect |
|---|---|
| `edit_letter` | Operator overrides letter fields; new revision |
| `regenerate_letter` | Recompose from current inputs; append revision |
| `skip_prospect` | Exclude from Ready-to-Print / campaign PDF |
| `mark_address_invalid` | Force Needs Review; flag missing address |
| `replace_recipient` | Swap recipient name; may revalidate |
| `approve_package` | Mark package approved when validation passes |

### Campaign summary metrics

- Prospects (total in scope)
- Ready to Print
- Needs Review
- Missing Addresses
- Estimated Print Time (≈ 12s / ready letter, deterministic)
- Estimated Assembly Time (≈ 45s / ready package × insert count factor)

## Data Model

```text
packages/capabilities/mail/
  types.js
  validate.js
  personalize.js
  render.js
  exportCsv.js
  exportDocx.js
  MailPackageStore.js
  MailPackageGenerator.js
  index.js
```

### Package status

```ts
type PackageStatus = 'ready_to_print' | 'needs_review' | 'skipped' | 'approved'
```

### Per-prospect MailPackage

```ts
interface MailPackage {
  id: string
  prospectId: string
  status: PackageStatus
  letter: Letter
  envelope: Envelope
  personalizationSummary: PersonalizationSummary
  insertChecklist: InsertItem[]
  confidence: number
  warnings: string[]
  revision: number
}
```

### Letter / Envelope

```ts
interface Letter {
  recipientName: string
  companyName: string
  personalizedOpening: string
  valueProposition: string
  cta: string
  signature: string
  body: string
}

interface Envelope {
  recipientName: string
  companyName: string
  mailingAddress: string
  returnAddress: string
}
```

## Implementation Plan

1. File SPEC-033 + types / validate / personalize / render / CSV / DOCX HTML
2. MailPackageGenerator capability + in-memory revision store
3. Register builtin; mission type `mail_package_generation`; IntentRouter patterns
4. Tests for validation gates, exports, revision append
5. Later: Puppeteer PDF wrapper, Postgres store, Mission Memory pin, Execution Direct Mail consume

## Migration Strategy

- No required migration in v1 (in-memory store, like early ProposalStore)
- Forward: `mail_package_versions` table mirroring `proposal_versions` when durability is required
- SPEC-032: map artifacts → `activeArtifacts`; regenerate → append revision

## Testing

- Unit: validate Ready / Needs Review matrix; letter composition from playbook; CSV columns
- Capability: generate packages from approved campaign prospects; skip / invalid address; revision++
- Mission: IntentRouter + planner chain for `mail_package_generation`
- Manual: print HTML letter + envelope in browser print dialog

## Acceptance Criteria

- [x] Complete package generated for every approved prospect in scope
- [x] Missing data blocks Ready-to-Print (Needs Review)
- [x] Combined campaign printable HTML generated (Campaign PDF path)
- [x] Mail merge CSV generated
- [x] Address label CSV generated
- [x] Revision history preserved on regenerate
- [x] Ready-to-Print status only after validation passes
- [x] Operator review actions exposed on review package
- [x] Campaign summary counts Ready / Needs Review / Missing Addresses + time estimates

## Future Work

- Puppeteer individual + combined PDF binaries
- Full OOXML `.docx` via library
- Playbook-authored insert kits and return-address blocks
- Command Deck print-queue UI
- Mission Memory integration (SPEC-032)
- Execution Engine Direct Mail waiting tasks (SPEC-029)
- Company Intelligence decision-maker enrichment (SPEC-030)

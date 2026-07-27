# SPEC-034 — Campaign Review Workspace

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v1.3.0 |
| **Priority** | Critical |
| **Owner** | Pulseforge engineering |
| **Created** | 2026-07-27 |
| **Version** | v1.0.0 |
| **Depends on** | SPEC-022, SPEC-023, SPEC-028, SPEC-029 (Execution consumes approved revision), SPEC-030 (Company Intelligence — optional detail), SPEC-032 (Mission Memory — decisions / revisions), SPEC-033 (Mail Package batch), Campaign Builder outputs, ADR-003, ADR-010, ADR-011, ADR-014, ADR-015, ADR-016, ADR-021 |
| **Consumed by** | Mission Engine, Command Deck Operations, Max Workspace, Execution Engine (latest approved campaign revision only) |

## Objective

Provide a single operator workspace for reviewing, editing, approving, and rejecting all campaign artifacts before execution.

The Campaign Review Workspace is the **final human checkpoint** before a campaign becomes executable. No outbound action may occur without campaign approval ([ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md)).

## Vision References

- `docs/vision/Mission.md`
- `docs/vision/Product_Constitution.md`
- [ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md) — human approval before execution
- [ADR-003](../adr/ADR-003_Human_Approval.md) — customer-visible actions require approval
- [ADR-010](../adr/ADR-010_Mission_Engine.md) — Mission Engine
- [ADR-011](../adr/ADR-011_Capability_Framework.md) — capabilities as the stable API
- [ADR-014](../adr/ADR-014_Personalized_by_Default.md) — personalization facts visible in review
- [ADR-015](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md) — playbook-owned voice / inserts
- [ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) — Execution consumes approved revision only
- [SPEC-029](SPEC-029_Execution_Engine.md)
- [SPEC-030](SPEC-030_Company_Intelligence_Capability.md)
- [SPEC-032](SPEC-032_Mission_Memory.md)
- [SPEC-033](SPEC-033_Mail_Package_Generator.md)

## Problem

Mail Package Generator (SPEC-033) produces print-ready artifacts, and Campaign Builder drafts strategy — but operators lack a **unified review surface** that:

1. Shows campaign summary + prospect queue in one place
2. Surfaces Company Intelligence, signals, opportunity brief, and personalization facts per prospect
3. Supports inline letter edit, regenerate, restore, and bulk approve / reject
4. Blocks approval on validation failures
5. Transitions the campaign to Ready to Print only after explicit operator approval
6. Records revision history and Mission decisions

Without Campaign Review, approval is fragmented across mail package overrides and mission review flags — and Execution cannot safely pin a single approved revision ([ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md)).

## Scope

- Campaign Review Workspace capability (`campaign_review`) as a first-class Mission capability
- Mission type `campaign_review`
- Inputs: Mission context, Approved Campaign, Company Intelligence (optional), Mail Package Batch, Client Playbook
- Layout model (operator-facing view model):
  - **Campaign Summary** — name, client, discovery profile, generated date, revision, prospect / ready / needs-review / blocked counts
  - **Prospect Queue** — status, company, recipient, score, confidence, personalization, letter, address, last modified; sort by score / confidence / needs review / alphabetical
  - **Prospect Detail** — company intelligence, summary, signals, opportunity brief, evidence; personalization facts; editable letter preview; envelope preview; editable insert checklist
- Per-prospect actions: Approve · Reject · Skip · Edit Letter · Regenerate · Replace Recipient · Update Address · Add Operator Note
- Bulk actions: Approve Selected · Reject Selected · Regenerate Selected · Export Selected · Print Selected
- Validation gates before prospect approval and campaign Ready-to-Print
- Campaign approval → Ready to Print + execution package (print package, mail merge, address labels)
- Revision history: number, timestamp, operator, change summary; compare / restore / duplicate
- Mission integration: every approval → Mission Decision; every regeneration → Mission Revision (shaped for SPEC-032)
- Artifacts shaped for Mission Memory `activeArtifacts` when SPEC-032 lands

## Out of Scope

- Full Command Deck HTML UI chrome (v1 returns the workspace view model + printable exports; UI binds later)
- Live Puppeteer PDF binary in the hot path (reuses SPEC-033 printable HTML)
- Execution Engine launch / ship confirmation ([SPEC-029](SPEC-029_Execution_Engine.md))
- Full Mission Memory tables ([SPEC-032](SPEC-032_Mission_Memory.md)) — local revision store mirrors the contract
- Inventing addresses, recipients, or company data (fail closed)

## Dependencies

| Dependency | Role |
|---|---|
| SPEC-023 Capability Framework | Capability contract + registry |
| Campaign Builder | Approved campaign artifact |
| SPEC-033 Mail Package Generator | Letter / envelope / insert / CSV / HTML batch |
| SPEC-028 Client Playbook | Brand voice, offers, return identity, insert kit |
| SPEC-030 Company Intelligence | Detail pane enrichment (optional) |
| SPEC-032 Mission Memory | Pin decisions + revisions on Mission |
| SPEC-029 Execution Engine | Consumes latest **approved** campaign revision only |
| ADR-021 | Human approval before execution |

## Architecture

```text
Mission (active)
      ↓
Approved Campaign · Mail Package Batch · Playbook · Company Intelligence
      ↓
Campaign Review Workspace
      ↓
Prospect Queue + Detail + Validation
      ↓
Operator actions (per / bulk) → append revision
      ↓
Campaign Approve (gates pass) → Ready to Print
      ↓
Execution Package (print · mail merge · labels)
      ↓
Mission Decision + Mission Revision (SPEC-032 shape)
      ↓
Execution Engine (SPEC-029) — latest approved revision only
```

### Design rules

1. **Generation ≠ approval** — Mail Package / Campaign Builder produce artifacts; Review validates; Approval authorizes execution ([ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md)).
2. **Fail closed** — missing address / company / recipient, validation failed, or confidence below threshold → cannot approve prospect.
3. **Campaign Ready to Print only when** every required prospect is approved, no blocking validation errors, mail package generated, and the current revision is active.
4. **Execution consumes latest approved revision only** ([ADR-016](../adr/ADR-016_Execution_Does_Not_Decide.md) / [ADR-021](../adr/ADR-021_Human_Approval_Before_Execution.md)).
5. **Deterministic v1** — same inputs + same actions → same workspace / revision summary (stable for tests).

### Validation (prospect cannot be approved if)

| Check | Effect |
|---|---|
| Missing address | Blocked |
| Missing company | Blocked |
| Missing recipient | Blocked |
| Validation failed (mail package / letter) | Blocked |
| Confidence below configured threshold | Blocked |

### Campaign approval gates

| Gate | Required |
|---|---|
| Every required (non-skipped) prospect approved | Yes |
| No blocking validation errors | Yes |
| Mail package batch present | Yes |
| Current revision is the active revision | Yes |

### Review actions

| Action | Effect |
|---|---|
| `approve` | Approve prospect when validation passes |
| `reject` | Reject prospect; exclude from Ready-to-Print |
| `skip` | Skip prospect; not required for campaign approval |
| `edit_letter` | Inline letter override; append revision |
| `regenerate` | Recompose from mail package inputs; append revision |
| `replace_recipient` | Swap recipient; revalidate |
| `update_address` | Update mailing address; revalidate |
| `add_note` | Operator note on prospect |
| `approve_selected` / `reject_selected` / `regenerate_selected` | Bulk variants |
| `export_selected` / `print_selected` | Export / printable subset |
| `approve_campaign` | Transition to Ready to Print when gates pass |
| `restore_revision` / `duplicate_revision` | Revision history ops |

## Data Model

```text
packages/capabilities/campaignReview/
  types.js
  validate.js
  assemble.js
  actions.js
  CampaignReviewStore.js
  CampaignReviewWorkspace.js
  index.js
```

### Campaign review status

```ts
type CampaignReviewStatus =
  | 'in_review'
  | 'ready_to_print'
  | 'rejected'
  | 'blocked'
```

### Prospect review status

```ts
type ProspectReviewStatus =
  | 'needs_review'
  | 'approved'
  | 'rejected'
  | 'skipped'
  | 'blocked'
```

### Workspace summary

```ts
interface CampaignReviewSummary {
  campaignName: string
  client: string | null
  discoveryProfile: string | null
  generatedAt: string
  revision: number
  prospectCount: number
  readyCount: number
  needsReviewCount: number
  blockedCount: number
  status: CampaignReviewStatus
}
```

### Prospect queue row

```ts
interface ProspectQueueRow {
  prospectId: string
  status: ProspectReviewStatus
  company: string
  recipient: string
  score: number
  confidence: number
  personalization: string[]
  letterPreview: string
  address: string
  lastModified: string
  validationErrors: string[]
  operatorNote: string | null
}
```

## Implementation Plan

1. File SPEC-034 + ADR-021 + types / validate / assemble / actions / store
2. Campaign Review Workspace capability + register builtin
3. Mission type `campaign_review`; IntentRouter patterns; playbook pin
4. Tests: validation blocks, per/bulk approve, campaign Ready-to-Print gates, revision history
5. Later: Command Deck UI, Mission Memory pin, Execution consume approved revision

## Migration Strategy

- No required migration in v1 (in-memory store, like MailPackageStore)
- Forward: `campaign_review_revisions` table when durability is required
- SPEC-032: map approvals → Mission Decisions; regenerations → Mission Revisions; pin `activeArtifacts`

## Testing

- Unit: prospect approval validation matrix; campaign gates; sort helpers
- Capability: assemble workspace from campaign + mail packages; approve / reject / skip; bulk approve; block Ready-to-Print until gates pass; revision++
- Mission: IntentRouter + planner chain for `campaign_review`
- Manual: inspect workspace view model JSON + printable export subset

## Acceptance Criteria

- [x] Single review workspace for entire campaign
- [x] Per-prospect review (approve / reject / skip / edit / regenerate / replace / address / note)
- [x] Bulk approval workflow
- [x] Inline editing
- [x] Full revision history (compare / restore / duplicate shapes)
- [x] Validation blocks prospect approval
- [x] Campaign transitions to Ready to Print only after operator approval and gates pass
- [x] Execution package (print / mail merge / labels) emitted on campaign approval
- [x] Mission Decision + Mission Revision shapes on approve / regenerate

## Future Work

- Command Deck Campaign Review UI (queue + detail panes)
- Mission Memory live attach (SPEC-032)
- Execution Engine launch from Ready-to-Print revision (SPEC-029)
- Company Intelligence live detail hydrate (SPEC-030)
- Diff UI for revision compare

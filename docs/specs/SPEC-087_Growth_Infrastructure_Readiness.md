# SPEC-087 — Growth Infrastructure Readiness Conversation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High (P1) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-08 |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md); [SPEC-086 Growth Conversation](SPEC-086_Growth_Conversation.md) (sibling, not a dependency of execution); [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

After Blueprint approval, Max can run a **separate** conversation that evaluates whether a business has the connective tissue needed to receive, convert, and track growth — **before** campaigns or prospecting begin.

**Core question:** Is this business ready to turn demand into measurable opportunities?

This is **not** market prioritization (that is SPEC-086 Growth Conversation). It is deeper operational setup: domain/DNS, website, GBP, reviews, social, tracking, lead capture, CRM/pipeline, sales process, and brand assets.

Success for v1: after Blueprint approval, an operator/client can start Growth Infrastructure Readiness on `/client-intel`, answer a short diagnostic flow, and receive a **Growth Infrastructure Readiness Report** that distinguishes what Max can check automatically, what can be operator-guided, and what requires client/operator action — with a clear setup sequence and **no campaign generation**.

## Vision References

- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md)
- [SPEC-086 Growth Conversation](SPEC-086_Growth_Conversation.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)

## Problem

Growth Conversation asks where to focus first. Even with a sharp first segment, demand can leak if the business cannot capture, convert, or measure inbound interest. CIE today has no dedicated post-approval beat for:

1. Domain / DNS / branded email readiness
2. Website conversion basics
3. GBP claim and completeness
4. Reviews and reputation process
5. Social profile consistency
6. Analytics and source tracking
7. Lead capture and missed-lead process
8. CRM / pipeline hygiene
9. Sales estimate / proposal process
10. Brand assets needed for outreach credibility

Without that assessment, Max risks recommending growth activity before the business can catch the ball.

## Scope

- Guided **Growth Infrastructure Readiness Conversation** on `/client-intel` after Blueprint approval
- Separate from Growth Conversation (market focus) — parallel post-approval mode
- Assess readiness areas: Domain/DNS, Website, GBP, Reviews, Social, Tracking, Lead Capture, CRM/Pipeline, Sales Process, Brand Assets
- Conversational diagnostic flow (not a long form)
- Each checklist item supports:
  - `status`: `ready` | `partial` | `missing` | `unknown` | `not_applicable`
  - `evidence`
  - `owner`: `max_can_check` | `operator_guided` | `client_required`
  - `priority`: `high` | `medium` | `low`
  - `recommended_next_step`
- Artifact: **Growth Infrastructure Readiness Report**
- Persist on `interview_state` (JSONB); no new DB enum required
- APIs: `POST …/readiness/start`, `POST …/readiness/message`
- Rules: no DNS/GBP/social/tracking mutation without explicit approval; never ask for passwords; prefer OAuth/connected flows; keep client truth separate from automated observations until reviewed; do not generate campaigns

## Out of Scope

- Changing DNS, GBP, social profiles, or tracking without explicit approval
- Asking for passwords in chat
- Campaign copy, prospect lists, or Scout/Emmett activation
- Mutating the approved Blueprint
- Full Growth Planning workspace
- Automated live DNS/GBP/analytics probes beyond structured “max_can_check” placeholders (v1 records owner + recommended next step; live connectors may land later)
- Replacing SPEC-086 Growth Conversation

## Dependencies

- SPEC-083 approved Business Blueprint + session persistence
- SPEC-084 `/client-intel` phase machine + completion state
- Sibling to SPEC-086 (either conversation may run after approval; neither blocks the other)

## Architectural invariant

> Growth Infrastructure Readiness is operational setup assessment. It cites conversation answers and optional automated observations. It stops before execution. It does not change DNS, GBP, social, or tracking without approval, and it does not generate campaigns.

```text
Executive Business Brief
  → Business Blueprint (edit / approve)
  → Initial Growth Direction (directional read)
  → Growth Infrastructure Readiness Conversation  ← this spec
       and/or Growth Conversation (SPEC-086)
  → Growth Infrastructure Readiness Report
  → [later] setup work / campaign planning (out of scope here)
```

## Conversation flow

### Entry

Requires:

- Interview session with an **approved** Blueprint
- User action: **Check Growth Infrastructure** (completion CTA alongside Start Growth Conversation)

Phase: `readiness` on `/client-intel`.

### Suggested flow

1. Opening: “Before we create demand, I want to make sure [Business] can capture and convert it. I’ll check the basics first.”
2. Ask for website/domain
3. Ask whether GBP exists and is claimed
4. Ask how leads currently arrive and where they go
5. Ask how estimates/proposals are handled
6. Ask what tracking exists today
7. Ask what assets are ready
8. Produce the Growth Infrastructure Readiness Report when enough answers are in (or when the user asks Max to wrap / report)

### Flow control

- One question at a time; brief acknowledgements
- Multi-dimension answers extract what’s covered and skip asked gaps
- Unknown is valid — prefer `unknown` over guessing
- Report may regenerate as answers improve
- Never ask for passwords; for client-required claiming/verification, state the action and owner

## Readiness areas

| Area id | Label |
|---|---|
| `domain_dns` | Domain and DNS |
| `website` | Website |
| `gbp` | Google Business Profile |
| `reviews` | Reviews and Reputation |
| `social` | Social Profiles |
| `tracking` | Tracking and Analytics |
| `lead_capture` | Lead Capture |
| `crm_pipeline` | CRM and Pipeline |
| `sales_process` | Sales Process |
| `brand_assets` | Brand Assets |

Each area contains checklist items (see implementation `READINESS_AREAS`). Item owners:

| Owner | Meaning |
|---|---|
| `max_can_check` | Max/system can observe via URL, public page, or connected account (no password ask) |
| `operator_guided` | Operator can walk the client through setup in-product or with guided steps |
| `client_required` | Client/operator must claim, verify, or supply (e.g. GBP verification, DNS registrar access) |

## Output artifact

### Growth Infrastructure Readiness Report

`kind: 'growth_infrastructure_readiness_report'`

Sections:

| Section | Content |
|---|---|
| **Overall readiness status** | Aggregate: `ready` / `partial` / `not_ready` / `unknown` |
| **Demand capture risks** | High-priority gaps in lead capture, phone/email routing, forms, missed-lead process |
| **Trust / discoverability gaps** | Website, GBP, reviews, social, brand proof |
| **Tracking gaps** | Analytics, pixels, Search Console, call/form/CRM source tracking, UTMs |
| **Conversion / follow-up gaps** | CRM stages, estimate/proposal process, follow-up cadence |
| **What Max can check automatically** | Items with `owner: max_can_check` |
| **What the operator/client must complete** | `operator_guided` + `client_required` outstanding items |
| **Recommended setup sequence** | Ordered next steps before campaign execution |

Item shape:

```json
{
  "id": "website_exists",
  "label": "Website exists",
  "status": "ready|partial|missing|unknown|not_applicable",
  "evidence": "",
  "owner": "max_can_check|operator_guided|client_required",
  "priority": "high|medium|low",
  "recommended_next_step": "",
  "source": "client_stated|automated_observation|operator_note|unknown"
}
```

Client-stated truth and automated observations remain labeled separately (`source`) until an operator reviews them.

## Architecture

```text
Approved Blueprint
        │
        ▼
POST /readiness/start  → opening + question bank cursor
        │
        ▼
POST /readiness/message → capture answers → update item statuses → when complete:
        │
        ▼
buildGrowthInfrastructureReadinessReport(areas, answers, blueprint)
        │
        ▼
interview_state.infrastructureReadiness + interview_state.growthInfrastructureReadinessReport
        │
        ▼
/client-intel phase readiness → report panel
```

Primary modules:

| Layer | Module |
|---|---|
| Domain | `services/clientIntelligenceInfrastructureReadiness.js` |
| Session | `services/clientIntelligenceInterview.js` — `startInfrastructureReadinessConversation` / `postInfrastructureReadinessMessage` |
| HTTP | `routes/clientIntelligence.js` |
| UI | `public/client-intel.html` — phase `readiness`, CTA, report renderer |
| Tests | `test/clientIntelligenceInfrastructureReadiness.test.js` |

## Data model

No schema migration for v1. Persist on `cie_interview_sessions.interview_state`:

```json
{
  "infrastructureReadiness": {
    "status": "active|report_ready",
    "startedAt": "ISO-8601",
    "step": "opening|website_domain|gbp|lead_flow|estimates|tracking|assets|report",
    "answers": {},
    "areas": {},
    "turns": [],
    "context": { "blueprintId": "", "blueprintVersion": "" }
  },
  "growthInfrastructureReadinessReport": {
    "kind": "growth_infrastructure_readiness_report",
    "title": "Growth Infrastructure Readiness Report",
    "overallStatus": "ready|partial|not_ready|unknown",
    "demandCaptureRisks": [],
    "trustDiscoverabilityGaps": [],
    "trackingGaps": [],
    "conversionFollowUpGaps": [],
    "maxCanCheck": [],
    "operatorClientMustComplete": [],
    "recommendedSetupSequence": [],
    "areas": {},
    "disclaimer": "Assessment only — no DNS, GBP, social, or tracking changes were made."
  }
}
```

Session APIs return logical status `INFRASTRUCTURE_READINESS`. Approved Blueprint immutability unchanged.

## Rules (non-negotiable)

1. **No DNS / GBP / social / tracking changes** without explicit approval
2. **Never ask for passwords** in chat
3. **Prefer OAuth / connected account flows** where possible
4. **GBP verification and social claiming** are `client_required` unless official integrations exist
5. **Client truth ≠ automated observation** until reviewed (`source` field)
6. **No campaigns, prospect lists, or autonomous outreach** from this report
7. **Unknown over invention** when evidence is thin

## Implementation plan

1. Spec + registry (this document; README; CURRENT_STATE; CHANGELOG)
2. Domain service: readiness areas, question bank, answer extraction, report builder
3. Session APIs + routes
4. UI phase + CTA + report panel
5. Tests (Anchor-oriented fixture + acceptance assertions)
6. Mark Implemented when acceptance checklist passes

## Migration strategy

None required. New `interview_state` keys default when absent. Growth Conversation state untouched.

## Testing

- Opening names business and states capture/convert intent (no campaign language)
- Step progression covers website → assets
- Report includes all required sections
- Item statuses and owners populate correctly from answers
- Forbidden: password asks, DNS mutation claims, campaign generation
- UI markers for Check Growth Infrastructure + report sections

## Acceptance criteria

- [x] Max can guide a business through infrastructure readiness after Blueprint approval
- [x] System distinguishes `max_can_check` / `operator_guided` / `client_required`
- [x] Output is a Growth Infrastructure Readiness Report with overall status, risk/gap sections, Max-vs-client work, and recommended setup sequence
- [x] Separate from Growth Conversation (market prioritization)
- [x] No campaign generation; no password asks; no unapproved infrastructure mutations
- [x] Available from `/client-intel` completion state

## Future work

- Live public checks (website fetch, SSL, basic meta, GBP public lookup) under `max_can_check`
- OAuth-connected GA / Search Console / GBP read-only probes
- Operator review UI to promote automated observations into client truth
- Soft link from accepted readiness report into setup task list / Playbook notes
- Gate campaign planning until high-priority capture risks are cleared

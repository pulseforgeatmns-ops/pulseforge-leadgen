# SPEC-089 — First Campaign Planning Conversation

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | High (P1) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-09 |
| **Depends on** | [SPEC-083](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-086](SPEC-086_Growth_Conversation.md); [SPEC-087](SPEC-087_Growth_Infrastructure_Readiness.md); [SPEC-088](SPEC-088_Growth_Work_Continuation_Flow.md); [ADR-021 Human Approval Before Execution](../adr/ADR-021_Human_Approval_Before_Execution.md); [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

After Blueprint approval, Growth Conversation focus selection, Growth Infrastructure review, and Growth Plan checklist completion, Max helps the operator plan the **first campaign** in a review-first conversation — without creating a prospect list, writing outreach copy, sending anything, mutating CRM rows, or changing accounts/DNS/GBP/social/tracking.

**Core question:** What is the campaign hypothesis, and what would prove this first test is worth pursuing?

Success for v1: selecting **Plan First Campaign** from Growth Plan completion opens the campaign planning conversation on `/client-intel`, Max carries forward approved artifacts (does not re-run the whole interview), clearly states this is planning not launch, and produces a **First Campaign Plan Preview** artifact only.

## Vision References

- [SPEC-086 Growth Conversation](SPEC-086_Growth_Conversation.md)
- [SPEC-087 Growth Infrastructure Readiness](SPEC-087_Growth_Infrastructure_Readiness.md)
- [SPEC-088 Growth Work Continuation Flow](SPEC-088_Growth_Work_Continuation_Flow.md)
- [SPEC-034 Campaign Review Workspace](SPEC-034_Campaign_Review_Workspace.md)
- [ADR-021 Human Approval Before Execution](../adr/ADR-021_Human_Approval_Before_Execution.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)

## Problem

Growth Plan completion options include **Plan First Campaign**, but the CTA only posts a chat stub. Operators finish setup work and have no guided beat to:

1. Confirm campaign objective
2. Confirm target segment and subtype
3. Confirm market bounds
4. Confirm proof assets available
5. Define campaign hypothesis
6. Define validation metrics
7. Define approval checkpoints
8. Produce a durable First Campaign Plan Preview

Without that beat, Max risks jumping from infrastructure readiness into list-building or copy before the hypothesis is agreed.

## Scope

- Guided **First Campaign Planning Conversation** on `/client-intel`
- Entry: Growth Plan completion option `launch_campaign` / **Plan First Campaign**
- Inputs reused from prior approved artifacts (no full re-interview):
  - Approved Blueprint
  - Initial Growth Direction
  - Segment ranking
  - Property Manager Validation Target (or equivalent validation target)
  - Growth Infrastructure Readiness Report
  - Completed setup checklist (`growthWork`)
- Conversation goals listed above
- Artifact: **First Campaign Plan Preview**
- Persist on `interview_state` JSONB; no new DB enum
- APIs: `POST …/campaign/start`, `POST …/campaign/message`
- Rules: review-first only — no prospect list, outreach copy, sends, CRM writes, or account/DNS/GBP/social/tracking changes

## Out of Scope

- Prospect list generation or Scout activation
- Outreach copy / sequences / Emmett / Sam / Bland
- Sends or scheduled launches
- CRM prospect/company writes beyond CIE session state
- DNS, GBP, social, tracking, or account mutations
- Mutating the approved Blueprint
- Full Campaign Builder / Campaign Review Workspace execution (SPEC-034 remains the later review surface)

## Dependencies

- SPEC-083 approved Business Blueprint + session persistence
- SPEC-086 First Growth Plan Preview / segment ranking / validation target
- SPEC-087 Growth Infrastructure Readiness Report
- SPEC-088 Growth Plan completion options (`launch_campaign`)

## Architectural invariant

> First Campaign Planning is hypothesis design. It cites prior approved artifacts and operator answers. It stops before execution. It does not create prospect lists, write outreach copy, send messages, or change accounts.

```text
Executive Business Brief
  → Business Blueprint (approve)
  → Initial Growth Direction
  → Growth Conversation → First Growth Plan Preview
  → Growth Infrastructure Readiness → Readiness Report
  → Growth Workspace / setup checklist complete
  → First Campaign Planning Conversation  ← this spec
  → First Campaign Plan Preview
  → [later] Campaign Builder / review / launch (out of scope)
```

## Conversation flow

### Entry

Requires:

- Interview session with an **approved** Blueprint
- User action: **Plan First Campaign** (Growth Plan completion option)

Phase: `campaign_planning` on `/client-intel`.

### Suggested opening

> Great. Anchor is ready to plan the first campaign. I’ll keep this review-first: no prospect list, outreach copy, or launch steps yet.
>
> We’re carrying forward the approved focus: property managers in Greater Manchester, with professional offices as a secondary path.
>
> Before anything gets built, I want to define the campaign hypothesis and what would prove this is worth pursuing.
>
> Should we plan around property managers exactly as defined, or do you want to narrow the first test further?

(Business name, primary segment, market, and secondary path are filled from prior artifacts.)

### Suggested steps

1. Confirm campaign objective
2. Confirm target segment and subtype
3. Confirm market bounds
4. Confirm proof assets available
5. Define campaign hypothesis
6. Define validation metrics
7. Define approval checkpoints
8. Produce First Campaign Plan Preview

### Flow control

- One question at a time; brief acknowledgements
- Multi-dimension answers extract what’s covered and skip asked gaps
- Max uses prior artifacts instead of re-asking the Growth Conversation / readiness interview
- Preview may regenerate as answers improve
- Explicitly state planning-only framing in opening and preview disclaimer

## Output artifact

### First Campaign Plan Preview

`kind: 'first_campaign_plan_preview'`

Sections:

| Section | Content |
|---|---|
| **Campaign objective** | What the first test is trying to learn or prove |
| **Target segment** | Primary segment + subtype (e.g. property managers / best-fit subtype) |
| **Market bound** | Geography / market limits for the first test |
| **Hypothesis** | If we approach X in Y with Z proof, we expect W signal |
| **Proof assets needed** | Credibility assets required before outreach |
| **Validation metrics** | What counts as early proof (conversations, walkthroughs, etc.) |
| **Risks/cautions** | Capacity, proof gaps, infrastructure caveats |
| **Approval checkpoints** | Human gates before list/copy/launch |
| **Recommended next step** | Still pre-execution (e.g. operator review of preview) |

Flags: `planningOnly: true`, `campaignsGenerated: false`, `prospectListGenerated: false`, `outreachCopyGenerated: false`.

## Architecture

```text
Growth Plan complete → Plan First Campaign
        │
        ▼
POST /campaign/start  → opening + prior-artifact context
        │
        ▼
POST /campaign/message → capture answers → when complete:
        │
        ▼
buildFirstCampaignPlanPreview(...)
        │
        ▼
interview_state.campaignPlanning + interview_state.firstCampaignPlanPreview
        │
        ▼
/client-intel phase campaign_planning → preview panel
```

Primary modules:

| Layer | Module |
|---|---|
| Domain | `services/clientIntelligenceCampaignPlanning.js` |
| Session | `services/clientIntelligenceInterview.js` — `startCampaignPlanningConversation` / `postCampaignPlanningMessage` |
| HTTP | `routes/clientIntelligence.js` |
| UI | `public/client-intel.html` — phase `campaign_planning`, completion CTA wire-up, preview renderer |
| Tests | `test/clientIntelligenceCampaignPlanning.test.js` |

## Data model

No schema migration for v1. Persist on `cie_interview_sessions.interview_state`:

```json
{
  "campaignPlanning": {
    "status": "active|preview_ready",
    "startedAt": "ISO-8601",
    "step": "opening|campaign_objective|target_segment|market_bounds|proof_assets|hypothesis|validation_metrics|approval_checkpoints|preview",
    "answers": {},
    "turns": [],
    "context": {
      "blueprintId": "",
      "blueprintVersion": "",
      "primarySegment": "",
      "secondarySegment": "",
      "targetMarket": ""
    }
  },
  "firstCampaignPlanPreview": {
    "kind": "first_campaign_plan_preview",
    "title": "First Campaign Plan Preview",
    "campaignObjective": "",
    "targetSegment": "",
    "marketBound": "",
    "hypothesis": "",
    "proofAssetsNeeded": [],
    "validationMetrics": [],
    "risksCautions": [],
    "approvalCheckpoints": [],
    "recommendedNextStep": "",
    "planningOnly": true,
    "disclaimer": "Planning preview only — no prospect list, outreach copy, sends, or account changes."
  }
}
```

Session APIs return logical status `CAMPAIGN_PLANNING`.

## Rules (non-negotiable)

1. **No prospect list** generation
2. **No outreach copy** drafts for send
3. **No sends** or launch steps
4. **No CRM writes** beyond CIE `interview_state`
5. **No account / DNS / GBP / social / tracking changes**
6. **Carry forward** approved artifacts — do not re-run the whole interview
7. **Unknown over invention** when answers are thin
8. Preview is **not** an approved campaign

## Implementation plan

1. Spec + registry (this document; README; CURRENT_STATE; CHANGELOG; SPEC-088 Future Work)
2. Domain service: steps, opening, reply, preview builder, forbidden-language guard
3. Session APIs + routes
4. UI: wire Plan First Campaign CTA; phase + preview panel
5. Tests (Anchor-oriented + acceptance assertions)
6. Mark Implemented when acceptance checklist passes

## Migration strategy

None required. New `interview_state` keys default when absent. Prior growth/readiness state untouched.

## Testing

- Plan First Campaign opens campaign planning conversation
- Opening carries approved focus and states planning-not-launch
- Prior artifacts reused (Blueprint, growth direction, ranking, validation target, readiness report, completed checklist)
- Preview includes all required sections
- Forbidden: prospect list, outreach copy, sends, CRM mutation, account changes
- UI markers for Plan First Campaign + First Campaign Plan Preview

## Acceptance criteria

- [x] Selecting Plan First Campaign opens the campaign planning conversation
- [x] Max uses prior approved artifacts instead of asking the whole interview again
- [x] Max clearly says this is planning, not launch
- [x] The output is a preview artifact only
- [x] No campaign, prospect list, copy, send, or account mutation occurs

## Future work

- Soft gate: require high-priority readiness gaps cleared before start
- Handoff into Campaign Review Workspace (SPEC-034) with this preview as input
- Operator approve/reject of First Campaign Plan Preview as a durable gate
- Optional subtype picker UI for property-manager validation targets

# SPEC-086 — Growth Conversation v1

| Field | Value |
|---|---|
| **Status** | Draft |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-08 |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md); [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md); [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

After the Business Blueprint is approved and **Initial Growth Direction** is shown, Max guides the client into a focused **Growth Conversation**. The goal is to turn the approved Blueprint into a practical **First Growth Plan Preview** — choosing which first market segment to prioritize and why — without generating campaigns, prospect lists, or autonomous actions.

Positioning after “I understand your business”:

> Now let’s decide where to focus first.

**Core question:** Which first market segment should the business prioritize, and why?

Success for v1: Anchor (or any CIE client) finishes Blueprint approval → sees Initial Growth Direction → completes a short consultant-led Growth Conversation → receives a First Growth Plan Preview they can accept, compare, or refine — still pre-execution.

## Vision References

- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md)
- [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md)
- [SPEC-028 Client Playbook Capability](SPEC-028_Client_Playbook_Capability.md)
- [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)

## Problem

CIE today ends well for understanding (Brief → Blueprint → approve → Initial Growth Direction) but the post-approval Growth Conversation is only a thin v0: keyword-routed chat that restates Blueprint segments/markets/goals. It does not yet:

1. Compare possible first segments systematically
2. Pressure-test operational fit, access, deal quality, and capacity
3. Produce a durable **First Growth Plan Preview** artifact
4. Offer clear CTAs (“Use this focus” / compare / refine Blueprint / dashboard)

Without that beat, Max feels useful until Blueprint approval, then stalls before campaign/prospect planning. The “Growth Planning destination” deferred in SPEC-084/085 remains unresolved.

## Scope

- Guided Growth Conversation on `/client-intel` after Blueprint approval + Initial Growth Direction
- Inputs: **approved Blueprint facts only** + **answers from this Growth Conversation**
- Conversation goals:
  1. Compare possible first segments
  2. Clarify capacity and operational fit
  3. Clarify ease of reaching each segment
  4. Clarify urgency and likelihood of recurring revenue
  5. Identify the best first segment to validate
  6. Define what “good signal” looks like before building campaigns
- Fixed suggested question flow (consultant tone; may skip/merge when already answered)
- Generate **First Growth Plan Preview** after sufficient answers
- Preview CTAs: Use this focus · Compare another segment · Refine the Blueprint · Return to Dashboard
- Persist conversation turns + preview on `interview_state` (JSONB)
- Harden existing `POST …/growth/start` and `POST …/growth/message` to drive the v1 flow
- Anchor Cleaning regression fixture (Greater Manchester commercial segments)

## Out of Scope

- Prospect list generation
- Campaign copy (except optionally one short positioning angle in the preview)
- Autonomous outreach / Scout / Composer / Emmett activation
- CRM writes beyond CIE session state (unless a later explicit design adds them)
- Full Growth Planning workspace / destination product
- Mutating the approved Blueprint from Growth Conversation answers (refine CTA returns to Blueprint revise flow)
- Claiming the market is validated
- Playbook strategy fields inventing channels, offers, or sequences from this conversation

## Dependencies

- SPEC-083 approved Business Blueprint + session persistence
- SPEC-084 `/client-intel` phase machine + completion state
- SPEC-085 Brief precedes Blueprint (unchanged)
- Existing Initial Growth Direction builder (`services/clientIntelligenceGrowthDirection.js`)
- Existing growth APIs: `POST /api/v1/interview/:id/growth/start`, `POST /api/v1/interview/:id/growth/message`
- ADR-015: strategy remains in Playbook; this conversation chooses a **validation focus**, not an execution plan

## Architectural invariant

> Growth Conversation is understanding-led focus selection. It cites Blueprint facts and user answers. It stops before execution. It does not generate prospect lists, campaigns, or autonomous actions.

```text
Executive Business Brief
  → Business Blueprint (edit / approve)
  → Initial Growth Direction (directional read)
  → Growth Conversation v1
  → First Growth Plan Preview
  → [later] campaign / prospect planning (out of scope here)
```

## Conversation flow

### Entry

Requires:

- Interview session with an **approved** Blueprint
- Initial Growth Direction present (build on approve if missing)
- User action: **Start Growth Conversation** (existing completion CTA)

Phase: `growth` on `/client-intel`. Side panel may continue showing Initial Growth Direction until the First Growth Plan Preview replaces or supplements it.

### 1. Confirm the directional read

Max opens (illustrative Anchor copy; generalize from Blueprint segments + geography):

> Based on your Blueprint, I’d start by comparing a few recurring commercial segments in Greater Manchester: property managers, short-term rental companies, facility managers, professional offices, daycares, rec centers, and high-traffic buildings. I won’t build a campaign yet. First, I want to help you choose the sharpest starting point.

Rules:

- Segment list and geography come from Blueprint `idealCustomers` / `targetMarkets` (and normalized facts when present)
- Explicitly state: no campaign yet
- Tone: decisive consultant, not a menu of product features

### 2. Segment preference

> Which of these segments feels most attractive to you right now, and why?

### 3. Operational fit

> Which segment would [Business] be most confident serving well if demand appeared this month?

### 4. Access / reach

> Which segment do you already have the easiest path into through relationships, referrals, local knowledge, or existing examples?

### 5. Deal quality

> Which segment is most likely to become recurring work at the quality level you want, rather than one-off or lowest-price work?

### 6. Constraints

> Are there any segments that look attractive but would strain capacity, staffing, scheduling, or service quality right now?

### 7. Proof / assets

> What proof can [Business] already show for this segment: photos, testimonials, before/after examples, references, service checklists, or a clear process?

### 8. First Growth Plan Preview

After answers (or when the user asks Max to recommend / wrap), Max produces the preview artifact (see Output artifact).

### Flow control

- Questions may be asked one at a time; Max acknowledges briefly and advances
- If the user answers multiple dimensions in one turn, Max should extract what was covered and only ask remaining gaps
- If confidence is low, Max states what is missing rather than overstating a recommendation
- “Compare another segment” re-enters comparison without discarding prior answers; may regenerate preview
- “Refine the Blueprint” exits to Blueprint revise / resume paths (SPEC-083/084) — Growth Conversation does not silently rewrite approved sections

## Inputs

| Source | Fields |
|---|---|
| Approved Blueprint | business identity, services, ideal customers, customers to avoid, geography / target markets, competitive advantages, brand voice, campaign goals, success metrics |
| Initial Growth Direction | `firstFocus`, `segmentsToInspect`, `marketsToInspect`, `primaryArea`, `towns`, directional disclaimer |
| Growth Conversation answers | preference, operational fit, access, deal quality, constraints, proof/assets |

No external market validation data required for v1.

## Output artifact

### First Growth Plan Preview

`kind: 'first_growth_plan_preview'`

Suggested structure:

| Section | Content |
|---|---|
| **Recommended first segment** | Named segment from Blueprint-compatible list |
| **Why this segment** | Cites Blueprint facts + conversation answers (operational fit, access, recurring quality, proof) |
| **Why not the others first** | Secondary — not discarded forever; capacity / reach / fit reasons |
| **First validation target** | The sharpest near-term validation question (not a campaign brief) |
| **Early signals to watch** | What “good signal” looks like before building campaigns |
| **Risks and constraints** | Capacity, staffing, scheduling, quality, avoid-list tension |
| **What I would prepare next** | Pre-execution next step Max would prepare **if approved** (still no autonomous action) |

Optional: one short positioning angle (≤1–2 sentences) when useful — never full campaign copy.

### Confidence rule

If answers conflict or key dimensions are missing, preview must include an explicit **missing information** note and lower-confidence framing. Prefer “here’s what I’d validate next” over a false sense of certainty.

### CTA after preview

| CTA | Behavior |
|---|---|
| **Use this focus** | Mark preview `status: 'accepted'`; keep session on `/client-intel` completion; do **not** launch campaigns/lists |
| **Compare another segment** | Continue Growth Conversation; regenerate preview when ready |
| **Refine the Blueprint** | Route to Blueprint revise / interview resume (existing CIE paths) |
| **Return to Dashboard** | Navigate to `/dashboard` (or role home); preview remains on session state |

## Architecture

```text
Approved Blueprint + Initial Growth Direction
        │
        ▼
POST /growth/start  → opening (confirm directional read) + question bank cursor
        │
        ▼
POST /growth/message → capture answers → advance steps → when complete:
        │
        ▼
buildFirstGrowthPlanPreview(blueprint, growthDirection, answers)
        │
        ▼
interview_state.growthConversation + interview_state.firstGrowthPlanPreview
        │
        ▼
/client-intel phase growth → preview panel + CTAs
```

Primary modules:

| Layer | Module |
|---|---|
| Domain | `services/clientIntelligenceGrowthDirection.js` — extend with question bank, answer extraction, `buildFirstGrowthPlanPreview` |
| Session | `services/clientIntelligenceInterview.js` — `startGrowthConversation` / `postGrowthMessage` step state |
| HTTP | `routes/clientIntelligence.js` — existing growth routes; optional accept/compare CTA endpoints if UI needs them |
| UI | `public/client-intel.html` — guided steps, preview renderer, CTAs |
| Tests | `test/clientIntelligenceGrowthDirection.test.js` (+ routes/interview as needed) |

Max Workspace / dashboard Ask Max / `maxAgent` briefing are **not** the home for this conversation.

## Data model

No required schema migration for v1. Persist on `cie_interview_sessions.interview_state`:

```json
{
  "initialGrowthDirection": { "kind": "initial_growth_direction", "...": "..." },
  "growthConversation": {
    "status": "active|preview_ready|focus_accepted|comparing",
    "startedAt": "ISO-8601",
    "step": "confirm|preference|operational_fit|access|deal_quality|constraints|proof|preview",
    "answers": {
      "segmentPreference": { "segment": "", "why": "", "raw": "" },
      "operationalFit": { "segment": "", "raw": "" },
      "access": { "segment": "", "raw": "" },
      "dealQuality": { "segment": "", "raw": "" },
      "constraints": { "segments": [], "raw": "" },
      "proof": { "assets": [], "raw": "" }
    },
    "turns": [{ "speaker": "assistant|client", "message": "", "at": "", "step": "" }],
    "context": { "blueprintId": "", "blueprintVersion": "" }
  },
  "firstGrowthPlanPreview": {
    "kind": "first_growth_plan_preview",
    "title": "First Growth Plan Preview",
    "recommendedSegment": "",
    "whyThisSegment": [],
    "whyNotOthers": [],
    "firstValidationTarget": "",
    "earlySignals": [],
    "risksAndConstraints": [],
    "whatIWouldPrepareNext": [],
    "positioningAngle": null,
    "citations": {
      "blueprintSectionKeys": [],
      "growthAnswerKeys": []
    },
    "confidence": { "level": "high|medium|low", "missing": [] },
    "status": "draft|accepted|superseded",
    "directional": true,
    "disclaimer": "This is a first-focus plan for validation, not a campaign launch."
  }
}
```

Optional later: first-class `cie_growth_plans` table if previews must be versioned like blueprints. Not required for v1 acceptance.

Session status: continue returning logical `GROWTH_CONVERSATION` from growth APIs (as today). Do not require a new `SESSION_STATUSES` enum value unless product wants completion gating; approved Blueprint immutability remains unchanged.

## Rules (non-negotiable)

1. **No prospect list generation**
2. **No campaign copy** except optional one short positioning angle
3. **No autonomous outreach**
4. **No CRM writes** unless explicitly designed later
5. **Recommendations must cite** Blueprint facts and conversation answers
6. **Low confidence → say what’s missing** rather than overstate
7. **Avoid-list stays in force** — never recommend a first segment that contradicts Blueprint customers-to-avoid without calling out the tension
8. **Pre-strategy** — “What I would prepare next” is a proposal, not an executed action

## Implementation plan

1. **Spec + registry** — this document; README; CURRENT_STATE; CHANGELOG; point SPEC-084/085 Future Work here
2. **Question bank + step machine** in `clientIntelligenceGrowthDirection.js` (opening + steps 2–7 + completion detection)
3. **Answer capture** on `growthConversation.answers` via `postGrowthMessage`
4. **`buildFirstGrowthPlanPreview`** — deterministic synthesis from Blueprint + answers; citation fields required
5. **UI** — step-aware Growth Conversation; render First Growth Plan Preview; wire four CTAs
6. **Accept / compare** — `focus_accepted` vs re-enter compare without wiping useful answers
7. **Tests** — Anchor fixture end-to-end: open → answer dimensions → preview cites Blueprint + answers; assert no campaign/list language; low-confidence path when answers thin
8. **Docs heartbeat** — mark Implemented when acceptance checklist passes

## Migration strategy

None required. Forward-compatible enrichment of existing `interview_state.growthConversation`. Older thin v0 turns remain readable; new fields default when absent. Opening copy may replace v0 opening for new starts; resumed sessions keep prior turns.

## Testing

- `test/clientIntelligenceGrowthDirection.test.js`
  - Opening confirms directional read from Anchor Blueprint segments + Greater Manchester
  - Step progression covers preference → proof
  - Preview recommends a segment with citations to Blueprint + answers
  - Secondary segments explained; avoid-list respected
  - Missing-answer path yields low confidence + missing notes
  - Forbidden: prospect lists, campaign briefs, “market validated”, autonomous action claims
- `test/clientIntelligenceRoutes.test.js` — UI markers for First Growth Plan Preview + CTAs
- Manual smoke: `/client-intel` approve → Start Growth Conversation → complete → Use this focus

## Acceptance criteria

- [ ] Growth Conversation is available only after Blueprint approval + Initial Growth Direction
- [ ] Max opens by confirming the directional read and naming Blueprint-derived segments/geography
- [ ] Conversation covers preference, operational fit, access, deal quality, constraints, and proof (gaps explicit if skipped)
- [ ] First Growth Plan Preview is produced with all required sections
- [ ] Recommendations cite Blueprint facts and conversation answers
- [ ] No prospect lists, campaign copy packs, or autonomous outreach
- [ ] Low-confidence path states what is missing instead of overstating
- [ ] CTAs work: Use this focus · Compare another segment · Refine the Blueprint · Return to Dashboard
- [ ] “Use this focus” does not activate Scout, Composer, campaigns, or CRM prospect writes
- [ ] Anchor regression: Greater Manchester recurring commercial segments remain the comparison set when Blueprint says so
- [ ] Client feels Max is useful immediately after Blueprint approval by choosing a first market focus

## Future work

- Dedicated Growth Planning workspace (the destination deferred in SPEC-084/085)
- Versioned `cie_growth_plans` table + operator review
- Handoff from accepted First Growth Plan Preview into campaign/prospect planning capabilities
- LLM-assisted synthesis that still consumes only Blueprint + growth answers (no invented market proof)
- Multi-location / multi-brand segment comparison
- Soft write of accepted focus into Playbook review notes (operator-visible only; still not execution)

## Relationship to shipped thin slice

Already in repo (not sufficient for this spec’s acceptance):

- `buildInitialGrowthDirection`
- `buildGrowthConversationOpening` / `buildGrowthConversationReply` (keyword routing)
- `POST /api/v1/interview/:id/growth/start|message`
- `/client-intel` Start Growth Conversation CTA + phase `growth`

This spec **upgrades** that thin slice into Growth Conversation v1 with a real question bank and First Growth Plan Preview. Until implementation lands, status remains **Draft**.

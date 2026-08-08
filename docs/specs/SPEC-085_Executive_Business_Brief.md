# SPEC-085 — Executive Business Brief

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-07 |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md) |

## Objective

Deliver the **Executive Business Brief** — a client-facing, consultant-quality synthesis that is the first tangible demonstration Max understands the client's business.

Unlike the Business Blueprint (an operational artifact for Pulseforge), the Brief is written for the client. It should be something a business owner would proudly save, print, or forward to a partner.

Every Brief should leave the client thinking **"I feel understood"** before **"I received recommendations."**

## Vision References

- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md)
- [SPEC-028 Client Playbook Capability](SPEC-028_Client_Playbook_Capability.md)
- [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)

## Problem

SPEC-084 ships a read-only Executive Summary ("My Understanding of Your Business") before the Blueprint. That beat is right, but the artifact is still a short narrative — not a shareable consulting brief with observations, evidence-backed assessment, meaningful unknowns, and curiosity-building conversation starters.

Without that depth, clients can feel "summarized" rather than understood, and Max has not yet earned permission for recommendations.

## Scope

- Premium Understanding Transition (deliberate checklist; min 2.5s; target 3–4s; never stall after backend completes)
- Executive Business Brief generation from Blueprint section evidence (synthesis, not transcription)
- Nine Brief sections:
  1. Who You Are
  2. Who You Serve
  3. Why Customers Choose You
  4. Where You're Headed
  5. Success Looks Like
  6. Initial Observations (max five; evidence-connected; not recommendations)
  7. Max's Initial Assessment (star ratings + confidence %; explanations reference evidence)
  8. Areas I'd Like To Learn More (always identify meaningful unknowns)
  9. Conversations I'd Recommend Next (conversation starters, not prescriptions)
- Client validation actions: Yes / Refine / Keep talking
- `/client-intel` presentation as a premium consulting report (calm typography, generous whitespace)
- API field `executiveSummary` remains the Brief payload (backward-compatible name); UI titles it Executive Business Brief

## Out of Scope

- Editable Brief
- Recommendations / tactics / campaign strategy
- Blueprint content changes beyond what Brief synthesis consumes
- Voice interview
- Growth Planning destination (post-approval focus selection is [SPEC-086](SPEC-086_Growth_Conversation.md))
- Sharing / PDF export infrastructure

## Dependencies

- SPEC-083 CIE interview → evidence → Business Blueprint
- SPEC-084 phase machine, resume, premium load, client actions
- SPEC-028 playbook handoff after Blueprint approve (unchanged)

## Architecture

Generation flow:

```text
Interview Complete
  → Premium Understanding Transition
  → Executive Business Brief
  → Client Validation
  → Business Blueprint
  → Playbook Generation
```

`buildExecutiveSummary(sections)` (CIE service) synthesizes the Brief from Blueprint section summaries + confidence + unknowns. No new tables. No LLM required for v1 — deterministic consultant synthesis from evidence already on the Blueprint.

Before render, summaries pass through `sanitizeSectionsForBrief`: only `business_fact` evidence is kept; `refinement_feedback` / meta-instruction language is rejected. Refinement messages may be stored on `interview_state.revisionGuidance` and must not populate commercial Blueprint fields.

Presentation lives on `/client-intel` (phase `executive_summary` / Brief reveal).

## Data Model

No schema changes. Brief is a derived view over `cie_business_blueprints.sections` (and session `interview_state.sectionState` when Blueprint is first generated).

Brief payload shape (illustrative):

```json
{
  "title": "Executive Business Brief",
  "subtitle": "Prepared by Max",
  "tagline": "A working picture for leadership review",
  "sections": [
    { "id": "whoYouAre", "title": "Who You Are", "kind": "prose", "body": "…" },
    { "id": "observations", "title": "Initial Observations", "kind": "list", "items": ["…"] },
    {
      "id": "assessment",
      "title": "Max's Initial Assessment",
      "kind": "assessment",
      "ratings": [{ "label": "Business Clarity", "stars": 5, "explanation": "…" }],
      "confidencePercent": 89
    }
  ]
}
```

## Implementation Plan

1. Spec + README registry
2. Expand CIE Brief builder: observations, assessment, always-on unknowns, conversation starters; retitle
3. Update `/client-intel` Premium Understanding steps, Brief renderer, action labels, report styling
4. Tests + CURRENT_STATE / CHANGELOG

## Migration Strategy

None. Forward-compatible enrichment of existing `executiveSummary` response field.

## Testing

- `test/clientIntelligenceInterview.test.js` — Brief synthesis, no verbatim interview dump, always unknowns, assessment evidence
- `test/clientIntelligenceRoutes.test.js` — UI markers for Executive Business Brief + premium transition
- Existing handoff / approve paths unchanged

## Acceptance Criteria

- [x] Brief never repeats interview answers verbatim
- [x] Every section demonstrates synthesis rather than transcription
- [x] Implementation language / prompts / system terminology absent from client-facing copy
- [x] Refinement feedback / meta-instructions never populate commercial Brief fields (who you are / serve / choose / headed / success)
- [x] Pre-render sanitization rejects meta-instruction evidence snippets
- [x] Observations connect evidence instead of prescribing actions (max five)
- [x] Unknowns are always identified (never "nothing outstanding")
- [x] Assessment scores derive from observed section confidence — never fabricated; refinement instructions do not inflate ratings
- [x] Premium Understanding Transition feels deliberate (min 2.5s; no artificial stall after processing)
- [x] Client validation precedes editable Business Blueprint
- [x] Brief can be read independently and still provide meaningful value

## Future Work

- Print / share / PDF export
- Optional client annotations on Brief sections
- Adaptive Brief depth when interview is notes-only vs full discovery
- Soften trust-bridge copy into Brief intro when product prefers a single beat
- Optional LLM polish pass that still consumes only sanitized business_fact evidence

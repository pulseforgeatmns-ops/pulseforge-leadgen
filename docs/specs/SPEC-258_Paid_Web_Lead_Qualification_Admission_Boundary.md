# SPEC-258 — Paid Web Lead Qualification Admission Boundary

## Purpose

Bridge durable website-origin prospects (SPEC-257) into an explicit operator
qualification review without admitting them into setter, AO, or revenue
opportunity state machines.

## Flow

```
walkthrough_request → linked prospect → lead_created evidence
  → lead_qualification_review (agent_actions, pending)
  → operator decision: QUALIFY | NURTURE | DISQUALIFY
  → durable disposition + opportunityCreationReady flag
```

## Implementation

| Component | Location |
|---|---|
| Review ensure + decision | `lib/leadQualificationReview.js` |
| Walkthrough hook | `lib/walkthroughCapture.js` |
| Operator API | `POST /api/v1/lead-qualification-reviews/:id/decision` |
| Tests | `test/spec258LeadQualificationReview.test.js` |

## Non-goals

No opportunity creation, setter_status mutation, AO leads, walkthrough
scheduling, or revenue writes.

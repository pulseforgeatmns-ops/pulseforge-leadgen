# SPEC-084 — Client Intelligence Interview Experience

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-06 |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-028 Client Playbook](SPEC-028_Client_Playbook_Capability.md) |

## Objective

Transform the Client Intelligence Interview from a functional onboarding flow into Pulseforge's signature product experience.

This spec introduces no new intelligence or architecture. It improves presentation, trust, clarity, and emotional impact while preserving the existing Client Intelligence Engine.

Every client should finish the interview feeling genuinely understood before seeing recommendations.

## Vision References

- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [SPEC-028 Client Playbook Capability](SPEC-028_Client_Playbook_Capability.md)
- [ADR-015 Strategy Lives in the Playbook](../adr/ADR-015_Strategy_Lives_in_the_Playbook.md)

## Problem

The CIE v1 UI jumps from Q&A straight into an editable Blueprint. That skips the trust beat: Max should demonstrate understanding before asking the client to approve an implementation artifact.

## Scope

- Welcome → Discovery → Understanding → read-only Executive Summary → editable Business Blueprint → Playbook prep → on-page completion
- Fixed-height conversation + sticky composer + independent blueprint/understanding scroll
- Live understanding progress (titles, confidence, unknowns) without narrative summaries mid-interview
- Reflections every 2–3 answers (consultant tone)
- Interruptible premium loading (min 2.5s; never stall after backend completion)
- Trust bridge copy before Executive Summary reveal
- Read-only Executive Summary titled **My Understanding of Your Business**
- Resume interview after “refine” / “keep talking”
- Living completion state on `/client-intel` (no Growth Planning navigation yet)

## Out of Scope

- Voice / realtime transcription
- Adaptive LLM questioning
- Campaign generation
- Blueprint evolution / recalibration product
- Workspace / Growth Planning destination
- Editable Executive Summary
- Mid-interview narrative summary streaming

## Dependencies

- SPEC-083 interview APIs and CIE service
- SPEC-028 playbook handoff on blueprint approve

## Architecture

Presentation-layer phase machine on `/client-intel`, with thin API support:

- `understanding` — redacted section progress (no summaries)
- `executiveSummary` — read-only Max narrative after discovery completes
- `POST /api/v1/interview/:id/resume` — CLIENT_REVIEW → DISCOVERY for refinement

```text
Welcome
  → Discovery (+ live understanding progress)
  → Interruptible premium load
  → Trust bridge
  → Executive Summary (read-only)
  → Business Blueprint (editable)
  → Approve → Playbook prep
  → Completion on /client-intel
```

## Data Model

No schema changes. Uses existing `cie_*` tables and `interview_state` JSONB.

## Implementation Plan

1. Spec + README registry
2. CIE service: understanding progress, executive summary, resume
3. Route wiring
4. `/client-intel` phase UI
5. Tests

## Migration Strategy

None. Forward-compatible API fields only.

## Testing

- `test/clientIntelligenceInterview.test.js` — redaction, executive summary, resume
- `test/clientIntelligenceRoutes.test.js` — resume route + UI markers
- `test/clientIntelligenceHandoff.test.js` — approve path unchanged

## Acceptance Criteria

- [x] Conversation never expands the page vertically
- [x] Executive Summary is presented read-only before Blueprint edit/approve
- [x] Mid-interview panel shows titles/confidence/unknowns only (no summaries)
- [x] Premium loading is interruptible (min 2.5s; no artificial stall after backend)
- [x] Trust bridge appears before Executive Summary
- [x] Blueprint summarizes rather than copies interview responses
- [x] Confidence explains itself (hover/focus); internal reasoning never shown
- [x] Blueprint approval precedes playbook generation
- [x] Completion stays on `/client-intel` with living-foundation copy and Return to Dashboard / Finish

## Future Work

- Growth Planning destination after completion
- Adaptive questioning
- Voice interview
- Blueprint recalibration sessions

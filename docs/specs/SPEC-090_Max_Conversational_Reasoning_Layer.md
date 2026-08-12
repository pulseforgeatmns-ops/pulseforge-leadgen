# SPEC-090 — Max Conversational Reasoning Layer

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-10 |
| **Depends on** | [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md); [SPEC-084 Client Intelligence Interview Experience](SPEC-084_Client_Intelligence_Interview_Experience.md); [SPEC-085 Executive Business Brief](SPEC-085_Executive_Business_Brief.md); [SPEC-086 Growth Conversation](SPEC-086_Growth_Conversation.md); [SPEC-089 First Campaign Planning Conversation](SPEC-089_First_Campaign_Planning_Conversation.md); [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md) |

## Objective

Make Max behave less like a form workflow and more like an intelligent interviewer by adding a session-level reasoning layer **before** question handling, extraction, and artifact generation.

## Vision References

- [SPEC-083 Client Intelligence Engine](SPEC-083_Client_Intelligence_Engine.md)
- [ADR-045 Evidence Before Reasoning](../adr/ADR-045_Evidence_Before_Reasoning.md)

## Problem

CIE discovery already classifies some non-answers (correction, supplemental, refinement), but:

- Classification is incomplete (no approval / skip / insufficient_answer / clarification_request as first-class types).
- Vague answers still advance the question bank.
- Session memory does not track question debt, pending corrections, or evidence supporting each section in one place.
- Artifact generation can fire without checking required evidence.
- Synthesis still risks stitching prompt echoes when evidence is thin.

## Scope

- Unified message classification for CIE discovery (and shared helpers for growth/campaign):
  - `direct_answer`, `correction`, `add_on`, `approval`, `clarification_request`, `insufficient_answer`, `off_topic`, `skip`
  - Retain `refinement_feedback` for operator instructions (never business facts)
- Session reasoning memory on `interview_state.reasoningMemory`
- Cross-section routing (add-ons / corrections update the right prior section)
- One focused probing follow-up for vague / incomplete / contradictory / operationally important answers
- Artifact readiness gates for Blueprint, Growth Direction, Campaign Preview, Prospect Criteria
- Synthesis helpers that rewrite into clean business language
- Preserve guardrails: no campaigns, lists, outreach, CRM writes, DNS/GBP/social/tracking, or account changes without explicit approval

## Out of Scope

- LLM-adaptive free-form questioning (fixed question bank remains)
- Replacing Growth / Campaign keyword detectors wholesale (shared helpers only)
- Voice / realtime transcription
- Changing playbook handoff or Scout activation rules

## Dependencies

- SPEC-083 interview APIs and CIE service
- Existing `interview_state` JSONB (no schema migration)

## Architecture

```text
User message
  → Reasoning Layer (classify + session memory + cross-section route)
  → Sufficiency / probe (optional, stays on question)
  → Existing extractors (normalizedFacts / sectionState)
  → Artifact readiness check
  → Synthesis (clean business language) → artifact
```

Module: `services/clientIntelligenceReasoning.js`  
Integration: `services/clientIntelligenceInterview.js` (`postInterviewMessage`, blueprint generation)

## Data Model

No new tables. Extend `cie_interview_sessions.interview_state` JSONB:

```js
reasoningMemory: {
  acceptedFacts: [{ section, substance, at, source }],
  pendingCorrections: [{ section, substance, at, status }],
  openQuestions: [{ questionId, section, reason }],
  confidenceBySection: { [section]: number },
  evidenceBySection: { [section]: [string] },
  questionDebt: [{ questionId, section, reason, at }],
  activeProbe: { questionId, section, prompt, reason } | null,
  artifactsGenerated: [string], // ordered kinds emitted this session
  lastClassification: string | null,
}
```

## Implementation Plan

1. Spec + registry updates
2. Reasoning module (classify, memory, probe, readiness, synthesis)
3. Wire into discovery `postInterviewMessage`
4. Artifact readiness before Blueprint / shared checks for growth & campaign artifacts
5. Tests for acceptance criteria

## Migration Strategy

None. Forward-compatible JSONB fields only. Existing sessions without `reasoningMemory` initialize on next message.

## Testing

- `test/clientIntelligenceReasoning.test.js` — classification, memory, probes, readiness, synthesis
- `test/clientIntelligenceInterview.test.js` — add-on cross-section, vague probe, operator instructions, artifact non-repeat

## Acceptance Criteria

- [x] User can provide an add-on to an earlier answer and Max updates the correct section
- [x] User can answer vaguely and Max asks a useful probing question
- [x] Max does not repeat the same artifact when asked for the next one
- [x] Max does not treat operator instructions as client business facts
- [x] Generated artifacts read like synthesized business judgment, not stitched form responses
- [x] Guardrails preserved (no execution without explicit approval)

## Future Work

- LLM-assisted sufficiency scoring
- Adaptive question bank ordering driven by question debt

## Related: Max Synthesis Layer (2026-08)

Shared modules under `services/maxSynthesis/`:

| Module | Role |
|---|---|
| `MessageIntentClassifier` | Shared intents (`direct_answer`, `approval`, `approval_plus_next_request`, `correction`, `add_on`, …) |
| `ConversationMemoryUpdater` | Routes add-ons/corrections/approvals into the right memory section |
| `BusinessFactNormalizer` | Phrase-safe canonical fields (`targetSegmentPhrase`, `marketBoundPhrase`, …) |
| `ArtifactSynthesisContext` | Renderers consume normalized phrases only; raw answers stay on `evidence` |
| `OperatorChatResponsiveness` | Operator instruction priority + draft revision / stale-source diagnostics |
| `ConversationalResponsePolicy` | Post-workflow response modes (see [SPEC-091](SPEC-091_Max_Conversational_Response_Policy.md)) |

Growth Direction, Campaign Preview, Prospect Criteria, and Prospect List Build Proposal all attach `synthesisPhrases` from this path. Build Proposal approach copy embeds phrases — never raw prior artifact paragraphs.
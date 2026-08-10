# SPEC-091 — Prospect List Build Proposal & Artifact Progression

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-10 |
| **Depends on** | [SPEC-089 First Campaign Planning Conversation](SPEC-089_First_Campaign_Planning_Conversation.md); [SPEC-090 Max Conversational Reasoning Layer](SPEC-090_Max_Conversational_Reasoning_Layer.md) |

## Objective

After Prospect List Criteria Preview is approved, Max must advance to a **Prospect List Build Proposal** instead of replaying the criteria artifact — especially when the user combines approval with a next-step ask.

## Problem

User: “Approved. Before we build anything, tell me how you would approach building the first prospect list for this test…”

Expected: Prospect List Build Proposal  
Actual (pre-fix): Max repeated Prospect List Criteria Preview

## Scope

- Extend message classification with `approval_plus_next_request` and `artifact_request`
- Detect approval + next-request in one turn; mark prior artifact approved; advance
- Add Prospect List Build Proposal artifact (planning-only)
- Artifact replay guard for approved artifacts
- Session memory: `lastArtifactType`, `lastArtifactStatus`, `approvedArtifacts`, `nextRecommendedArtifact`, `pendingUserRequest`, `questionDebt`
- Wire classification into campaign planning message loop

## Out of Scope

- Actually building a prospect list
- Outreach copy, sends, CRM writes, DNS/GBP/social/tracking/account changes

## Acceptance Criteria

- [x] Approval-only does not replay criteria unless user asks to revise/view again
- [x] `approval_plus_next_request` advances to Prospect List Build Proposal
- [x] “Approved. Before we build anything…” produces Prospect List Build Proposal
- [x] Approved Prospect List Criteria Preview is not repeated
- [x] Guardrails preserved (no list/outreach/account changes without approval)

## Testing

- `test/clientIntelligenceReasoning.test.js` — intent + progression helpers
- `test/clientIntelligenceCampaignPlanning.test.js` — acceptance path for build proposal
- `test/maxSynthesisLayer.test.js` — shared Max Synthesis Layer phrases + Build Proposal snapshot (bans raw prompt stitching)

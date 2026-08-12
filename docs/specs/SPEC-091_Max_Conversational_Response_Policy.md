# SPEC-091 — Max Conversational Response Policy

| Field | Value |
|---|---|
| **Status** | Implemented (v1) |
| **Target Version** | v0.1 |
| **Priority** | Critical (P0) |
| **Owner** | Pulseforge |
| **Created** | 2026-08-12 |
| **Depends on** | [SPEC-090 Max Conversational Reasoning Layer](SPEC-090_Max_Conversational_Reasoning_Layer.md); OperatorChatResponsiveness; Campaign Planning review-artifact chain |

## Objective

Make Max respond like a conversational operator assistant **after** workflow and state decisions, without dumping workflow objects unless the operator asks for a formal gate, evidence, or checklist.

## Core Principle

Workflow state informs Max’s answer — it is not Max’s answer.

## Response Modes

| Mode | When | Wire `responseMode` |
|---|---|---|
| `operator_state_update` | State changed or gate already approved | `operator_state_summary` |
| `operator_revision_response` | Operator asked for copy/targeting/memory changes | `operator_chat_response` |
| `operator_diagnostic` | Stale state, conflicts, missing data, repeated failure | `stale_source_diagnostic` |
| `formal_review_gate` | First-time gate for approval | `workflow_review_card` |
| `execution_confirmation` | About to send/export/CRM/account | `execution_confirmation` |

## Module

`services/maxSynthesis/ConversationalResponsePolicy.js`

Integration points:

- Launch Gate approved-state formatting (`formatOutreachLaunchGateApprovedSummary`)
- Stale-source diagnostics (`buildStaleSourceDiagnostic`)
- Draft revision voice (`formatOperatorChatDraftResponse`)
- Post-readiness execution asks (`produceExecutionConfirmationResult`)
- `applyConversationalPolicy` on campaign replies

## Composition Rules

Before responding, Max answers:

1. What did the operator just ask or imply?
2. What state are we in now?
3. Did the state change?
4. First-time review / already-approved / revision / diagnostic / execution?
5. Shortest useful response?
6. What must stay blocked?
7. What should the operator do next?

## Do Not Sound Like a Renderer

Avoid outside formal review / execution modes:

- Recommended decision
- What is included
- Why this is recommended
- Primary actions
- View evidence
- Full sourced records
- Does this look right to approve

## Safety Without Repetition

Compact lock line on ordinary turns. Full expanded list only at formal gates or execution confirmation.

## Acceptance Criteria

- [x] Approved Launch Gate uses Operator State Update (not review card)
- [x] Approved-state responses use a single canonical acknowledgment (no stacked header/leadIn/summary)
- [x] Revision turns avoid renderer boilerplate
- [x] Diagnostics lead with plain language
- [x] Formal review gate still allowed for first-time approval
- [x] Execution asks require explicit confirmation and never auto-run
- [x] Mode selection sits after workflow/state decisions

## Testing

- `test/conversationalResponsePolicy.test.js`
- Launch Gate assertions in `test/prospectBatchReview.test.js`

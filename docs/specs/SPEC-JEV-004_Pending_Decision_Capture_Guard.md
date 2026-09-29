# SPEC-JEV-004 — Pending Decision Capture Guard

Status: implemented; deployment is a normal code rollout after SPEC-JEV-003.

## Objective and scope

Pending-decision resolution must capture an operator message only when the
message is actually a decision response. The production failure was:

```text
Operator: What is the current status and confidence of the Anchor STR mission?
Max: I didn't catch a clear yes or no for the pending decision. Approve discovery?
```

The operator asked a mission status/inspection question. Max treated it as an
unclear yes/no reply to the pending discovery approval.

This spec does not give Jev routing control, remove pending decisions, or
auto-approve/auto-reject. Deterministic text rules are sufficient. Optional
synchronous Jev/shadow metadata may only prevent capture of non-decision
messages.

## Runtime behavior

`packages/max/workspace/pendingDecisionCaptureGuard.js` classifies the operator
message before the pending-decision path can demand yes/no:

- `decision_response` — existing resolver still approves, rejects, holds, or modifies
- `pending_decision_clarification` — explain the pending decision; do not resolve it
- `inspection_or_status_question` / `unrelated_or_question` / long `ambiguous` —
  release turn ownership and continue normal routing
- `ambiguous_short_response` — keep the existing yes/no clarification

Guard failures are caught. They log
`[PENDING_DECISION_CAPTURE_GUARD_ERROR]` and fall back to the previous
pending-decision path. They cannot crash `WorkspaceEngine.ask`.

When capture is prevented, Max emits:

```text
[PENDING_DECISION_CAPTURE_GUARDED] {...}
```

The payload includes `event`, `spec`, `session_id`, `tenant_id`, `mission_id`,
`pending_decision_id`, `classification`, `reason`, and `message_chars`. The
raw operator message is not logged.

## Jev

Jev remains shadow-only. If a completed shadow decision is already present and
says `status_check` / `inspection` at high confidence, the guard may refuse
pending capture. Jev is never awaited on the response path and cannot block an
explicit approval or rejection.

## Validation

```sh
node --test packages/max/workspace/tests/specJev004PendingDecisionCaptureGuard.test.js
npm run test:max
```

Tests prove the Anchor STR status question is not swallowed, explicit
approve/hold paths still work, clarification questions are explained, prospect
questions fall through, short ambiguous replies still ask for yes/no, classifier
throws cannot crash routing, and optional Jev metadata is an additional guard
only.

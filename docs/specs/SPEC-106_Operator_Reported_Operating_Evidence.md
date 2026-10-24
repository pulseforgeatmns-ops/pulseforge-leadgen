# SPEC-106 — Operator-Reported Operating Evidence

| Field | Value |
|---|---|
| **Status** | Implemented (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Max Core Reasoning |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-103](SPEC-103_Durable_Business_Understanding_Retrieval.md), [SPEC-104](SPEC-104_Persistent_Operator_Context.md), [SPEC-105](SPEC-105_Max_Operating_Evidence_Retrieval.md) / PR #317 |

## Objective

Give Command Deck Max a write-side bridge for operator-reported operating evidence: recognize an operating update, extract bounded assertions, classify epistemic and temporal state, persist eligible evidence with provenance, rebuild derived operator context, and recover that evidence in a fresh Max session.

PulseForge does not treat conversational history as operating memory. Operator-reported business events become durable only when recognized, semantically classified, provenance-preserved, policy-permitted, and written to an appropriate canonical operating/evidence store.

## Core Principle

`operator_attested ≠ system_observed ≠ inferred`

Jake saying Campaign 001 was mailed August 6 is not the same as PulseForge independently observing that mailing, and not the same as PulseForge inferring that it probably happened.

## Architecture

```text
WorkspaceEngine.ask()
  specialist interrogation
  cognitive mode
  SPEC-105 retrieval-before-delegation
  SPEC-106 operating-update recognition
      extract → classify → resolve → persist → acknowledge
  Scout
  CIE
  ...
```

CIE must not claim a turn already recognized as `operating_update`.

## Persistence

Reuse Knowledge Evidence + Claim:

- Evidence `sourceType = operator_report`
- Claim metadata holds predicate, occurred_at / expected_at, epistemic state, actor, original wording
- Corrections invalidate the prior effective claim and keep the original evidence

Event A (campaign mail execution) is durable in v1. Event B (AO training) is acknowledged only. Event C (expected follow-up) may persist as an expected claim but must not silently mutate the AO cohort.

## Out of Scope

- General chat memory
- Treating operator assertions as independently verified facts
- Auto-completing plans when their date passes
- CIE Blueprint mutation
- Autonomous outreach or Scout launch
- Broad CIE typo-repair (`app` → `gap`); logged for later CIE cleanup now that SPEC-106 intercepts the Pilot path

## Testing

`packages/max/workspace/tests/operatorOperatingUpdate.test.js` and `test/operatorOperatingUpdate.test.js`.

## Acceptance Criteria

- [x] Operator declarative updates no longer fall into CIE
- [x] Multi-event Pilot message is decomposed
- [x] Completed vs expected is preserved
- [x] Operator-attested vs system-observed is preserved
- [x] Event A survives a fresh Max session
- [x] Event B is not forced into an inappropriate store
- [x] Event C cannot silently mutate 20 AO records
- [x] Expected dates never auto-become execution
- [x] Correction history is preserved
- [x] SPEC-105 retrieves persisted operator evidence with provenance
- [x] Tenant isolation remains fail-closed

# SPEC-JEV-003 — Operator-Visible Routing Mismatch Warnings

Status: implemented; deployment is a normal code rollout after SPEC-JEV-002.

## Purpose

PulseForge runs Jev as a production **shadow evaluator** ([SPEC-JEV-001](SPEC-JEV-001_Mission_Routing_Shadow_Evaluator.md)).
[SPEC-JEV-002](SPEC-JEV-002_Decision_Shadow_Review.md) makes those evaluations durable and reviewable.

SPEC-JEV-003 adds a safe warning layer: when Jev sees a likely routing mismatch, PulseForge
surfaces that mismatch to the operator/developer review path **without changing execution**.

This remains shadow-only. Jev must not route, reroute, block, retry, or override Max or
WorkspaceEngine behavior.

## Safety boundary

Jev may produce warnings only. Warning logic is best-effort and isolated:

- Does not change `current_route`, WorkspaceEngine results, or MissionEngine behavior
- Does not suppress Max responses, retry requests, trigger specialists, create approvals,
  or update mission state
- Does not cause request failure if warning logic errors
- Does not treat Jev confidence as ground truth

If warning generation fails, production routing and the operator response still succeed.

## Known production example

Operator message:

```text
What is the current status and confidence of the Anchor STR mission?
```

Observed shadow evaluation:

```json
{
  "event": "DECISION_SHADOW_EVALUATED",
  "status": "evaluated",
  "intent": "status_check",
  "confidence": 0.99,
  "inspection_probability": 0.93,
  "recommended_route": "inspection",
  "current_route": {
    "route": "conversation",
    "raw_route": "intelligence",
    "pipeline": "ClientIntelligence"
  },
  "route_matches": false,
  "comparison": "mismatch"
}
```

Production routed through conversation/intelligence. Jev classified the turn as mission
inspection/status with high confidence. SPEC-JEV-003 makes that gap visible for review.

## Threshold rules

Warnings are derived at query time by `classifyDecisionMismatch()` in
`packages/decision-service/mismatchClassifier.js`. A row becomes a warning candidate when
**all** of the following hold:

| Rule | Value |
| --- | --- |
| Status | `evaluated` with no provider errors |
| Mismatch | `comparison === 'mismatch'` or `route_matches === false` |
| Jev intent/route | `intent` in `status_check`, `inspection`, `mission_inspection`, or `recommended_route === 'inspection'` |
| Recommended route | `inspection` |
| Confidence | `>= 0.90` |
| Inspection probability | `>= 0.85` |
| Production route | `conversation`, raw route `intelligence`, or pipeline `ClientIntelligence` |
| Route failure | observed production route did not fail |

Do **not** warn on low confidence, low inspection probability, provider errors, fallback/noop
rows, rows where production already routed to inspection, or rows where Jev recommends
approval/rejection/content routes.

## Runtime surfacing

After each completed shadow evaluation, `DecisionService` classifies the row and, when matched,
emits a structured stdout warning:

```text
[DECISION_SHADOW_WARNING] {...}
```

The payload includes `warning_type: likely_mission_inspection_misroute`, `severity: review`,
correlation ids, route snapshots, and `action: review_current_route_without_changing_routing`.

Shadow evaluation remains asynchronous (post-response), so warnings are not attached to the
live operator response body. Review uses persistence + the scripts below.

## Review commands

Dedicated warning report (classifies stored rows at query time; no stored mutation):

```sh
npm run decision:review:warnings
node scripts/reviewDecisionShadowWarnings.js --limit 50
node scripts/reviewDecisionShadowWarnings.js --tenant 10 --json
```

SPEC-JEV-002 review with warnings filter (SQL pre-filter + same classifier in reports):

```sh
npm run decision:review -- --warnings --limit 50
npm run decision:review -- --tenant 10 --warnings --json
```

Both commands require `DATABASE_URL` and the SPEC-JEV-002 migration. They are SELECT-only
admin/developer paths.

## What warnings mean

- High-confidence evidence that Jev would route a turn to mission inspection/status while
  production routed it through conversation/intelligence
- A prompt for human review of routing heuristics, mission-boundary detection, or inspection triggers

## What warnings do not mean

- Jev is automatically correct
- Production routing was wrong
- The system should reroute, block, or retry the turn
- Operator approval or specialist execution is required

Automatic rerouting is an explicit non-goal. Any graduation beyond observation requires a
separate spec and review.

## Validation

```sh
npm run test:decision
npm run test:decision:postgres
```

Manual production validation after deploy:

1. Confirm shadow env vars (`DECISION_SHADOW_ENABLED`, `DECISION_PROVIDER`, `JEV_*`).
2. Send Max: *What is the current status and confidence of the Anchor STR mission?*
3. Confirm the normal Max response still returns with no route change.
4. Run `node scripts/reviewDecisionShadowWarnings.js --limit 50`.
5. Expect a warning candidate if production still routes through conversation/intelligence.

## Related specs

- [SPEC-JEV-001](SPEC-JEV-001_Mission_Routing_Shadow_Evaluator.md) — shadow evaluator
- [SPEC-JEV-002](SPEC-JEV-002_Decision_Shadow_Review.md) — durable review storage
- [SPEC-JEV-003 Shadow Routing Warning](SPEC-JEV-003_Shadow_Routing_Warning.md) — implementation notes (legacy filename)

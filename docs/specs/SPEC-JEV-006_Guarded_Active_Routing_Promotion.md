# SPEC-JEV-006 — Guarded Active Routing Promotion

Status: implemented; deployment is a normal code rollout after SPEC-JEV-005.

## Goal

Promote Jev from shadow evaluator to **active router** for safe read-only mission
inspection flows, while keeping all mutating and execution paths on the existing
production router.

## In scope

- `status_check` and `inspection_question` intents
- Mission inspection / status / “where are we at” operator questions
- Operator brief-style read-only questions when Jev selects the `intelligence` route

## Out of scope

Jev active routing must never promote:

- Sends, approvals, campaign state changes, AO assignment, outreach drafts
- CRM mutations or external actions
- `mission`, `approval`, `specialist`, `session_configuration`, or other mutating routes

## Runtime behavior

1. After `OperatorIntent` resolves (and mission snapshot is available), run a
   **synchronous** Jev evaluation when active routing is enabled.
2. If the decision is eligible (`confidence >= 0.90`, allowed intent/route, low
   risk, no approval/clarification signals), promote production ownership to the
   mapped read-only owner (`mission_inspection` or `knowledge_retrieval`).
3. Emit `DECISION_ACTIVE_ROUTING` with production owner/route, Jev route,
   confidence, and the final selected route.
4. On blocked, missing, low-confidence, or unsafe Jev output, keep the existing
   production router result.
5. Deferred SPEC-JEV-001 shadow evaluation continues unchanged for mismatch review.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `JEV_ACTIVE_ROUTING_ENABLED` | `false` | Exact `true` enables guarded promotion. |
| `JEV_ACTIVE_ROUTING_CONFIDENCE` | `0.90` | Minimum Jev route confidence for promotion. |
| `JEV_ACTIVE_ROUTING_INSPECTION_PROB` | `0.85` | Minimum inspection probability. |

Active routing requires the same Jev provider gates as shadow mode
(`DECISION_SHADOW_ENABLED`, `DECISION_PROVIDER=jev`, `JEV_ENABLED`, API key).

## Pending decision guard

Promotion may override `pending_decision_turn_ownership` only when
SPEC-JEV-004 classifies the message as `inspection_or_status_question`.
Explicit approvals and decision responses are never overridden.

## Validation

```bash
npm run test:decision
```

Includes `specJev006ActiveRoutingPromotion.test.js` and
`test/decisionActiveRouting.test.js`.

# SPEC-JEV-003 — Shadow Routing Warning

Status: implemented; deployment is a normal code rollout after SPEC-JEV-002.

## Objective and scope

Promote the production-validated Jev shadow mismatch pattern into a visible
operator/developer warning while keeping Jev in observer mode. SPEC-JEV-003 does
not change production routing, response text, mission state, approval state, or
execution behavior.

The first warning case is the Anchor STR pattern observed in production:

- Current route: `conversation` with raw route `intelligence`.
- Jev intent: `status_check`.
- Jev recommendation: `inspection`.
- Confidence or inspection probability at least `0.85`.
- Comparison: `mismatch`.

## Runtime behavior

`DecisionService` still schedules Jev work after the production response path.
When a completed shadow row matches the high-confidence mission-inspection
heuristic, the observer emits a second structured stdout row:

```text
[DECISION_SHADOW_ROUTING_WARNING] {...}
```

The warning payload is a safe projection of the shadow event:

```text
event, spec, schema_version, mode, severity, reason, decision_id, source,
session_id, tenant_id, mission_id, timestamp, current_route, intent, confidence,
inspection_probability, recommended_route, comparison, action
```

`action` is always `review_current_route_without_changing_routing`. The warning
does not contain the operator message, Max response, prompt, provider raw body,
credentials, SQL, or stack traces. If the warning sink throws or rejects, the
shadow row and production response still complete.

The warning is intentionally generated from the stored-review heuristic in
`packages/decision-service/shadowRoutingWarning.js`. Production routing does not
import it, and no method returns a Jev route to the workspace.

## Review command

The SPEC-JEV-002 review command now has a warnings filter:

```sh
npm run decision:review -- --warnings --limit 50
npm run decision:review -- --tenant 10 --warnings --json
```

`--warnings` selects the latest high-confidence likely mission-inspection rows
from `decision_shadow_events` using the same predicate as the runtime warning.
It is SELECT-only and remains an admin/developer path controlled by database
credentials. Counts still summarize only returned rows.

## Rollout and rollback

Roll out with the existing Jev shadow configuration. No migration is required
after SPEC-JEV-002 has already created `decision_shadow_events`.

Rollback options:

- Set `DECISION_SHADOW_PERSIST_ENABLED=false` to keep stdout shadow events but
  stop durable review writes.
- Set `DECISION_SHADOW_ENABLED=false` to stop shadow evaluation, persistence, and
  warning emission.
- Revert this code to remove `[DECISION_SHADOW_ROUTING_WARNING]` rows while
  keeping SPEC-JEV-002 persistence.

Do not add an automatic routing flag for this spec. The next graduation step, if
approved, should be a separately reviewed operator-facing UI annotation or a
bounded route-recommendation review surface.

## Validation

```sh
npm run test:decision
npm run test:decision:postgres
```

Tests prove the warning appears for the known production pattern, warning sink
failures cannot affect workspace results or persistence, `--warnings` returns
only the high-confidence review rows, and provider failures remain reported as
errors rather than warnings.

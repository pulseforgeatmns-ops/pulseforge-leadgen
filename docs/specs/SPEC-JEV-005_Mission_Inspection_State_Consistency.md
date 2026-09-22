# SPEC-JEV-005 — Mission Inspection State Consistency

## Purpose

SPEC-JEV-001 through SPEC-JEV-004 moved operator mission-status questions into the correct
`inspection` route (`owner = mission_inspection`, `route_matches = true`). Production Jev
evaluation confirmed routing is correct.

SPEC-JEV-005 fixes the **mission inspection answer layer** so status/confidence responses are
internally consistent, canonical, and grounded in mission state.

## Observed Production Failure

Operator asked:

```text
What is the current status and confidence of the Anchor STR mission?
```

Max responded:

```text
Scout finished its pass, but the mission is waiting on you — Approve discovery?
Here's why: Waiting for Scout
```

Jev attached the request to `mission_daily_0287c871bdea5ed6f3e59a1d` instead of the canonical
Anchor STR mission `mission_82e8102f-249c-4f44-b88e-2de76b13898e`.

These states cannot all be true simultaneously.

## Non-Goals

- Do not change Jev routing (DecisionService remains shadow-only).
- Do not auto-approve discovery or start Scout.
- Do not modify mission lifecycle unless existing execution flow already does.

## Implementation

### Canonical mission resolution

`packages/acquisition-mission/resolveInspectionMission.js`

When the operator names Anchor STR (or equivalent phrasing), inspection resolves the canonical
mission by id/objective before falling back to active or daily wrapper missions.

Returns:

```js
{
  mission,
  resolution_type: 'named_exact' | 'named_objective_match' | 'active_mission' | 'daily_wrapper' | 'not_found',
  requested_mission_label,
  canonical_mission_id,
  warnings: []
}
```

### Mission inspection snapshot

`packages/acquisition-mission/MissionInspectionSnapshot.js`

Single snapshot object built from `engine.inspect()` before prose generation:

| Field | Description |
|---|---|
| `mission_id` | Resolved mission id |
| `objective` | Canonical objective text |
| `resolution_type` | How the mission was resolved |
| `lifecycle_status` / `stage` | Mission lifecycle position |
| `confidence` | Grounded confidence or null |
| `waiting_on` / `waiting_reason` | Current blocker/wait state |
| `pending_decision` | Operator gate, if any |
| `last_completed_specialist_action` | Latest completed specialist work |
| `next_action` | Recommended next step |

### Contradiction validator

`validateMissionInspectionSnapshot(snapshot)` checks:

| Code | Severity | Rule |
|---|---|---|
| `specialist_finished_but_waiting_on_same_specialist` | error | Scout completed discovery but still waiting on Scout |
| `operator_waiting_reason_points_to_specialist` | error | `waiting_on = operator` but reason says Waiting for Scout |
| `discovery_approval_with_completed_scout_discovery` | error | Pending approve discovery with completed Scout discovery |
| `named_mission_resolved_to_daily_wrapper` | warning | Named Anchor STR resolved to `mission_daily_*` |

When severity `error` is present, Max emits conservative conflict disclosure instead of polished
contradictory prose.

Structured audit log:

```text
[MISSION_INSPECTION_STATE_INCONSISTENT] {"event":"MISSION_INSPECTION_STATE_INCONSISTENT","spec":"SPEC-JEV-005",...}
```

### Workspace integration

`packages/max/workspace/WorkspaceMissionInspection.js` resolves the mission, builds the snapshot,
validates it, and passes `inspectionSnapshot` / `inspectionValidation` to
`ConversationLayer.composeConversationalResponse()`.

`ConversationLayer` generates prose from the snapshot when present. Legacy explain-lead logic no
longer claims "Scout finished" for pending `discovery_approval`.

## Expected Responses

### Waiting on operator approval for discovery

```text
The Anchor STR mission is waiting on operator approval to begin Scout discovery.
Status: waiting for approval
Pending decision: approve discovery
```

No claim that Scout finished discovery.

### Scout completed discovery — operator review

```text
Scout completed discovery. The mission is now waiting on operator review/approval of the discovered prospects.
```

No "Waiting for Scout."

### State contradiction

```text
I found conflicting mission state for the Anchor STR mission.
...
Safest next step: Treat the mission as blocked pending state reconciliation...
```

### Daily wrapper fallback

When only a daily/watch mission exists:

```text
Note: I found this through the daily watch mission rather than the original Anchor STR mission record.
```

## Relationship to SPEC-JEV-004

SPEC-JEV-004 ensured routing and pending-decision capture reach mission inspection correctly.
SPEC-JEV-005 handles consistency **inside** the inspection answer layer once routing is correct.

## Manual Validation

After deploy:

```bash
# Ask Max in workspace:
What is the current status and confidence of the Anchor STR mission?

# Jev route should remain inspection/match:
railway logs | grep DECISION_SHADOW_EVALUATED

# Consistency log (only when actual contradiction exists):
railway logs | grep MISSION_INSPECTION_STATE_INCONSISTENT
```

Run tests:

```bash
node --test packages/max/workspace/tests/specJev005MissionInspectionConsistency.test.js
```

## Acceptance Criteria

1. Named mission inspection resolves canonical mission when possible.
2. Daily wrapper is not silently substituted for named Anchor STR mission.
3. Responses are generated from a single snapshot object.
4. Validator catches Scout-finished / Waiting-for-Scout contradictions.
5. Contradictory state produces conservative conflict disclosure.
6. Unknown confidence is disclosed as unavailable.
7. Jev routing remains unchanged.
8. Tests cover the observed production failure.

# AUDIT-019 — Lifecycle Transition Coverage

| Field | Value |
|---|---|
| **Status** | Completed (code-path proof; production mission id requires DB query) |
| **Date** | 2026-08-22 |
| **Related** | [SPEC-136](../specs/SPEC-136_Pending_Operator_Decision_Consistency.md), SPEC-137 (PR #373), [AUDIT-008](../../packages/max/workspace/tests/audit008AmoDiscoveryApprovalExecutionTrace.test.js) |
| **Scope** | Determine whether `MISSION_STATE_INCONSISTENT` originated from stale pre-SPEC-137 persisted state or an uncovered post-SPEC-137 lifecycle transition |

## Stop rule result

**Proven: (A) stale persisted state from pre-SPEC-137 code — not an uncovered post-SPEC-137 transition.**

Analysis stopped at category (A). No post-SPEC-137 code path mutates `mission.stage` without updating `pendingOperatorDecision` atomically.

---

## Deployment timestamps

| Milestone | Timestamp (UTC) | Commit / PR |
|---|---|---|
| SPEC-136 — `putMission` validation + `applyStageToPendingDecision` in `Engine.progress()` | 2026-08-22 03:13:37 | PR #372 (`9a1c21b`) |
| SPEC-137 — `applyStageTransition()` canonical lifecycle mutation | 2026-08-22 12:52:50 | PR #373 (`3db9556`) |

---

## Audit questions

### Mission ID

**Not present in repository artifacts.** Production occurrence surfaces only in runtime logs:

```text
[amo] hydrate skip inconsistent mission <mission_id> <message>
```

Query helper:

```bash
DATABASE_URL=... node scripts/audit019LifecycleTransitionCoverage.js [--mission-id <id>] [--tenant-id <id>]
```

Or SQL:

```sql
SELECT id, tenant_id, stage, created_at, updated_at,
       payload->'pendingOperatorDecision' AS pending
FROM acquisition_missions
WHERE payload->'pendingOperatorDecision' IS NOT NULL
  AND (
    (payload->'pendingOperatorDecision'->>'stage') IS DISTINCT FROM stage
    OR (payload->'pendingOperatorDecision'->>'kind' = 'discovery_approval' AND stage <> 'discover')
    OR (payload->'pendingOperatorDecision'->>'kind' IN ('plan_approval','plan_edit','plan_clarification') AND stage <> 'discover')
  );
```

### Mission created timestamp

Requires the query above against production `acquisition_missions.created_at`. Any inconsistent row necessarily predates the SPEC-136 `putMission` guard (2026-08-22 03:13 UTC) because new inconsistent writes are rejected at store write time.

### Was it created before or after SPEC-137 deployment?

**Expected: before.** Inconsistent rows could only be **written** while `Engine.progress()` mutated `stage`/`status` independently and `putMission` did not validate consistency (pre-SPEC-136). After SPEC-136, new inconsistent states cannot be persisted; after SPEC-137, stage transitions are atomic via `applyStageTransition()`.

A mission **created** after 2026-08-22 12:52 UTC that is inconsistent would contradict current code paths (see coverage matrix below) and would indicate direct DB tampering or a bug outside audited paths — none found.

### Was `Engine.progress()` from SPEC-137 executed for this mission?

**No (for stale missions).** Reason:

1. Hydrate loads persisted rows through `putMission()` → `assertMissionStateConsistent()`.
2. Inconsistent rows fail with `MISSION_STATE_INCONSISTENT` and are skipped (`services/acquisitionMission.js`).
3. The mission never enters the in-memory engine, so SPEC-137 `Engine.progress()` never runs.

If timeline events exist after SPEC-137 deploy, they belong to a **repaired or different** mission record — not the inconsistent payload that fails hydration.

### First lifecycle transition that mutated stage without updating `pendingOperatorDecision`

| Transition | Code path | Window | Cleared pending? |
|---|---|---|---|
| **`discover → understand`** | `Engine.progress()` direct (API `/api/v1/amo/missions/:id/progress`, tests, Max advance) | Pre-SPEC-136 (`6218160^`) | **No** — root cause |
| `discover → understand` | `commitDiscoveryStage()` → `engine.progress()` | SPEC-128+ | **Yes** — `clearPendingOperatorDecision()` runs before progress |
| Any stage change | `applyStageTransition()` | SPEC-137+ | **Yes** — atomic |

**First offending transition:** `discover → understand` via **`Engine.progress()` before SPEC-136**, which set:

```text
stage = understand
pendingOperatorDecision = { stage: discover, kind: discovery_approval | plan_approval }
```

This matches the failure mode described in PR #373 and reproduced in `packages/acquisition-mission/tests/spec137.test.js`.

---

## Post-SPEC-137 transition coverage matrix

| Mutator | File | Uses `applyStageTransition`? | Post-SPEC-137 gap? |
|---|---|---|---|
| `Engine.progress()` | `packages/acquisition-mission/Engine.js` | Yes | No |
| `applyStage()` (deprecated) | `packages/acquisition-mission/Lifecycle.js` | Yes (delegates) | No |
| `commitDiscoveryStage()` | `packages/max/workspace/AmoOperatorApproval.js` | Via `engine.progress()` | No |
| `store.restore()` | `packages/acquisition-mission/Store.js` | N/A (rollback/tests only; no validation) | Not a production write path |
| `persistMission()` | `services/acquisitionMissionPersistence.js` | N/A (SQL only; callers use `putMission` first) | No |

**Uncovered post-SPEC-137 lifecycle transition: none.**

---

## Why `commitDiscoveryStage()` was not the source

Since SPEC-128, discovery approval:

1. Clears `pendingOperatorDecision` (`clearPendingOperatorDecision`).
2. Attaches Scout discovery contribution.
3. Calls `engine.progress()` to advance when Scout is complete.

Even pre-SPEC-136, step 1 prevented stale pending from surviving the discover → understand transition on the **operator approval path**. Stale state came from **direct** `Engine.progress()` calls (progress API, test harnesses, manual Max advance) that bypassed approval commit.

SPEC-137 additionally removed swallowed `progress()` errors in `commitDiscoveryStage()` so stage advancement failures propagate instead of leaving Scout-complete missions stuck at `discover`.

---

## Reproduction

```bash
node scripts/audit019LifecycleTransitionCoverage.js
```

With `DATABASE_URL`, the script classifies each inconsistent mission and prints event timeline.

Manual repro (pre-SPEC-136 semantics on current validation):

1. Create mission, approve plan, attach Scout discovery.
2. Restore in-memory row with `stage=understand` and `pendingOperatorDecision.stage=discover`.
3. `putMission` throws `MISSION_STATE_INCONSISTENT: pendingOperatorDecision.stage does not match mission.stage.`

---

## Conclusion

| Hypothesis | Verdict |
|---|---|
| **(A) Stale persisted state from pre-SPEC-137 code** | **Proven** |
| **(B) Uncovered post-SPEC-137 lifecycle transition** | **Ruled out** — all production stage writes use `applyStageTransition()` since PR #373 |

**Remediation for affected missions:** repair or delete inconsistent rows in `acquisition_missions.payload` (set `pendingOperatorDecision = null` when `stage !== 'discover'`, or reset stage to `discover` and replay approval flow). New writes are guarded by SPEC-136/SPEC-137.

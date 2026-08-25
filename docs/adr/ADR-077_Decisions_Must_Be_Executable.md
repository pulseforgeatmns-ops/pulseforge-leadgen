# ADR-077 — Decisions Must Be Executable

| Field | Value |
|---|---|
| **Status** | Accepted |
| **Date** | 2026-08-25 |
| **Owners** | Max Runtime, Mission Engine, UI |
| **Related** | [ADR-058](ADR-058_Pending_Operator_Decision_Matches_Execution.md), [ADR-057](ADR-057_Transactional_Mission_Execution.md), [SPEC-136](../specs/SPEC-136_Pending_Operator_Decision_Consistency.md), [SPEC-141](../specs/SPEC-141_Discovery_Review_Gate.md) |

## Context

PulseForge presents operator decisions throughout mission execution (Approve Mission Plan, Approve Discovery, Approve Findings, Launch Campaign). During AUDIT-045 the runtime exposed a contract violation: the operator was presented **Approve findings**, which resolved to `prioritization_approved`, but TME rejected the transition because `hasSufficientEvidenceForPrioritization()` was false. The runtime behaved correctly; the conversation contract did not.

## Decision

1. Operator decisions become **executable contracts**. A decision may only be presented if every mandatory precondition for its corresponding business transition is already satisfied.
2. If execution would fail due to known preconditions, that decision must never be surfaced. PulseForge surfaces the **blocking decision** instead.
3. Every mission transition owns its readiness rules (`canApprovePlan`, `canApproveDiscovery`, `canApprovePrioritization`). UI components never duplicate these rules.
4. Rollback responses must describe the transaction that actually failed (e.g. "Prioritization could not execute", not "Discovery could not execute").
5. **Execution state** and **business readiness** are independent. Discovery complete does not imply ready for prioritization.

## Readiness Contract

```javascript
interface DecisionReadiness {
  executable: boolean;
  blockingReason?: string;
  missingEvidence?: string[];
  recommendedAction?: string;
  coveragePercent?: number;
}
```

Implemented in `packages/acquisition-mission/DecisionReadiness.js`. Evaluated in `presentableOperatorDecision()` before any operator decision is rendered.

## Discovery Example

When coverage is incomplete, the runtime sets `discovery_investigation` (not `prioritization_approval`) with actions: Continue Investigation, Modify Mission, Accept Incomplete Investigation.

When coverage is complete and evidence is sufficient, the runtime sets `prioritization_approval` with prompt **Approve findings?**.

## Consequences

### Positive

- Operators are never asked to approve an impossible transition.
- Presentation and TME share one readiness source of truth.
- Rollback copy identifies the failed business transition.

### Negative / tradeoffs

- Missions with stale `prioritization_approval` pending while evidence is insufficient fail closed with `MISSION_STATE_INCONSISTENT` on inspect.

### Follow-ups

- Wire **Accept Incomplete Investigation** to a dedicated TME path when operators choose to proceed with partial coverage.
- Extend readiness contracts to outreach launch (`canLaunchOutreach`).

## Acceptance Criteria

| Scenario | Expected |
|---|---|
| Coverage complete | **Approve Findings** is shown |
| Coverage incomplete | **Approve Findings** is not shown; **Continue Investigation** is presented |
| Operator selects displayed decision | TME does not fail precondition validation for known-at-render conditions |
| Rollback | UI identifies the transaction that failed |
| UI and TME | Evaluate the same readiness contract; no duplicated logic in presentation layer |

Tests: `packages/acquisition-mission/tests/adr077.test.js`

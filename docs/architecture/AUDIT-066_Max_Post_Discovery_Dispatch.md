# AUDIT-066 — Max Post-Discovery Dispatch

| Field | Value |
|---|---|
| **Status** | Remediated |
| **Date** | 2026-08-27 |
| **Related** | [SPEC-141](../specs/SPEC-141_Discovery_Review_Gate.md), [SPEC-132](../specs/SPEC-132_Specialist_Execution_Contract.md), [SPEC-131](../specs/SPEC-131_Transactional_Mission_Execution.md), [AUDIT-067](AUDIT-067_Paige_Post_Max_Dispatch.md) (downstream) |

## Executive summary

**First divergence:** After operator `PRIORITIZATION_APPROVAL`, `advancePrioritizationAfterApproval` consumed the approval and advanced the mission to `UNDERSTAND` without executing Max through the Specialist Execution Contract (SEC) or committing a `PRIORITIZATION` contribution.

Operators saw the stage advance with `maxComplete=false`, forcing a second side-channel (`advanceMaxPrioritization`) to attach Max cognition. That split violated SPEC-131 atomic commit semantics and left mission state implying Understanding without Max reasoning attached.

**Remediation:** `advancePrioritizationAfterApproval` now mirrors the Scout discovery pattern — TME execute → SEC validation → contribution contract validation → atomic commit (operator approval consumed + Max `PRIORITIZATION` attached + stage advanced to `UNDERSTAND`).

## Canonical flow (post-remediation)

```text
Scout DISCOVERY contribution
  ↓
Operator PRIORITIZATION_APPROVAL
  ↓
STAGES.UNDERSTAND — Max SEC execution (MaxPrioritizationExecutor)
  ↓
Validated PRIORITIZATION contribution
  ↓
mission at UNDERSTAND with maxComplete=true
```

## Trace

```text
advancePrioritizationAfterApproval()
  ↓  validatePrioritizationPreconditions (discovery artifact + evidence)
executeMissionStage({ specialist: MAX, stage: UNDERSTAND })
  ↓  runMaxForAmoMission()
MaxPrioritizationExecutor.runMaxPrioritization()
  ↓  buildExecutionInput + maxInput (structured mission + discovery)
executeSpecialist('max')
  ↓  SPEC-132 execution result
validatePrioritizationOutput()
  ↓  assertContributionContract(MAX) + assertExecutionResult
commitPrioritizationStage()
  ↓  OPERATOR approval + MAX PRIORITIZATION + engine.progress(UNDERSTAND)
```

## Fail-closed behavior

| Failure | Mission state | maxComplete |
|---|---|---|
| Invalid Max contribution contract | Stays at `DISCOVER` | false |
| Max `BLOCKED` / throw | Stays at `DISCOVER`, no PRIORITIZATION row | false |
| Success | `UNDERSTAND` with PRIORITIZATION committed | true |

## Backward compatibility

`advanceMaxPrioritization()` remains as an idempotent back-compat entry point. When Max already ran during prioritization approval, it returns `alreadyExecuted: true` without re-executing.

## Tests

- `packages/acquisition-mission/tests/audit066MaxPostDiscoveryDispatch.test.js` — happy path, contract failure, execution failure, ownership, Scout regression
- `packages/acquisition-mission/tests/spec067PaigePostMaxDispatch.test.js` — updated regression expects Max PRIORITIZATION at approval time

## Next downstream divergence (AUDIT-067)

Paige variant generation at `PLAN`/`PREPARE` — trace from committed Max `PRIORITIZATION` contribution toward Paige → Emmett (remediated separately on main).

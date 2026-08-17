# SPEC-102F — Max Development Framework

| Field | Value |
|---|---|
| **Status** | Foundation (v1 thin slice) |
| **Target Version** | v0.1 |
| **Priority** | Critical |
| **Owner** | Pulseforge |
| **Created** | 2026-08-17 |
| **Depends on** | [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md), [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md), [SPEC-100](SPEC-100_Max_Scout_Acquisition_Intelligence_Loop.md), [SPEC-101](SPEC-101_Max_Specialist_Result_Interrogation.md), [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md) |

> **Numbering note:** The product brief called this **SPEC-102 Foundation**. Repository SPEC-102 is [Max Retrieval Before Delegation](SPEC-102_Max_Retrieval_Before_Delegation.md). This development framework is **SPEC-102F**.

## Objective

Define how Max matures from a newly deployed intelligence into a trusted business operator through structured operational training — and provide a durable, inspectable record of what Max has learned.

PulseForge is not developed primarily by adding features. PulseForge is developed by teaching Max. Real operator work becomes structured training. Every production interaction is an opportunity to improve durable behavior. Max should graduate competencies rather than accumulate prompts.

**Core principle:** Train behavior, not outputs.

## Vision References

- [SPEC-098](SPEC-098_Max_Specialist_Delegation_Contract.md) — delegation discipline
- [SPEC-099A](SPEC-099A_Scout_Investigation_Provenance.md) — coverage evaluation
- [SPEC-101](SPEC-101_Max_Specialist_Result_Interrogation.md) — specialist trace interrogation
- [SPEC-102](SPEC-102_Max_Retrieval_Before_Delegation.md) — retrieve before delegation

## Problem

Feature specs alone do not answer the operator question that matters:

> What has Max learned?

Without a competency model, regressions look like prompt drift. Graduation is invisible. Failures get patched instead of becoming curriculum. New specialists force Max to relearn management behaviors from scratch.

## Scope

1. **Operator Training Loop** — documented contract every future Max spec should fit
2. **Competency registry** — durable catalog of behaviors, lifecycle stage, and spec links
3. **Training record** — inspectable history of graduated competencies (PulseForge-internal, not client-facing)
4. **Training exercise schema** — assignment, observed/expected behavior, lesson, retest, transfer test
5. **Performance review dimensions** — delegation, retrieval, judgment, evidence, communication, uncertainty, operator trust, reflection
6. **Regression suite hook** — graduated competencies map to existing automated tests
7. **CLI** — `node scripts/maxTrainingRecord.js` renders the training record

## Out of Scope

- Automated LLM coaching or live performance review generation
- Client-facing competency UI
- Database persistence (JSON registry + record file for v1)
- Synthetic benchmark harness (real work first; Anchor Cleaning priority)
- Renumbering existing implementation specs

## Philosophy

When Max behaves incorrectly:

1. Give him real work
2. Observe behavior
3. Interrogate reasoning
4. Identify the deficient principle
5. Encode the smallest durable lesson
6. Repeat the original assignment
7. Verify transfer to a different scenario
8. Graduate the competency

Failures are curriculum. Do not hide failures. Do not patch prompts. Do not optimize demos.

## Operator Training Loop

```text
Real Operator Work
        │
        ▼
Observe Behavior
        │
        ▼
Performance Review
        │
        ▼
General Principle
        │
        ▼
Implementation
        │
        ▼
Repeat Assignment
        │
        ▼
Competency Graduated
```

## Competency Lifecycle

| Stage | Meaning |
|---|---|
| `not_started` | Behavior not yet implemented |
| `training` | Behavior exists but requires coaching |
| `practicing` | Correct in many situations; still fails edge cases |
| `graduated` | Behavior consistently demonstrated |
| `regression` | Previously graduated behavior no longer reliable; requires retraining |

## Graduation Standard

A competency graduates only when:

1. It succeeds on the original assignment, **and**
2. It succeeds in a different scenario (transfer test)

Transfer matters more than memorization.

Example — **Retrieve Before Delegation**:

- Original: *What do you know about our service area?*
- Transfer: *What do you know about Kumho Tire?*

## Performance Reviews

Rather than asking *Is Scout done?*, ask *How did Max manage Scout?*

Review dimensions:

- Delegation
- Retrieval
- Judgment
- Evidence
- Communication
- Uncertainty
- Operator trust
- Reflection

## Real Work First

Priority order for training exercises:

1. Anchor Cleaning (`client_id=10`)
2. Pilot clients (Aji)
3. Production clients
4. Synthetic scenarios

Reality is the curriculum.

## Specialist Training Inheritance

Every new specialist inherits Max's management competencies. When Penny arrives, Max already knows retrieve, delegate, evaluate, explain, and inspect. Only Penny's domain knowledge is new. This keeps growth linear instead of exponential.

## Architecture

```text
packages/max/training/
  CompetencyLifecycle.js   — stage enum + transitions
  CompetencyRegistry.js    — canonical competency catalog
  TrainingExercise.js      — exercise schema validation
  TrainingRecord.js        — merged registry + record view
  PerformanceReview.js     — review dimension constants
  RegressionSuite.js       — graduated → test file mapping
  training-record.json     — graduation dates + exercise log
  index.js
scripts/maxTrainingRecord.js
```

Implementation specs (SPEC-098, SPEC-099A, SPEC-101, SPEC-102, …) encode individual competencies. SPEC-102F indexes them and tracks lifecycle.

## Data Model

### Competency

```json
{
  "id": "retrieve_before_delegation",
  "label": "Retrieve Before Delegation",
  "category": "core_management",
  "stage": "graduated",
  "graduatedAt": "2026-08-16",
  "specRefs": ["SPEC-102"],
  "regressionTests": ["test/retrievalBeforeDelegation.test.js"],
  "exercises": [{ "id": "service_area_retrieval", "assignment": "..." }]
}
```

### Training exercise

Required fields: `assignment`, `observedBehavior`, `expectedBehavior`, `failureMode`, `generalLesson`, `retest`, `transferTest`, `graduationDecision`.

## Testing

- `packages/max/training/tests/developmentFramework.test.js`
- Regression suite asserts every graduated competency has resolvable test paths
- Included in `npm run test:max`

## Acceptance Criteria

- [x] SPEC-102F documents the operator training loop and graduation standard
- [x] Competency registry lists core management competencies with lifecycle stages
- [x] Graduated competencies link to implementation specs and regression tests
- [x] `node scripts/maxTrainingRecord.js` renders an inspectable training record
- [x] Regression suite fails if a graduated competency loses its test mapping
- [x] Existing SPEC-098 / SPEC-099A / SPEC-101 / SPEC-102 tests remain green

## Future Work

- Postgres-backed training record with exercise attempt history
- Transfer-test automation per competency
- Performance review capture from live operator sessions
- Regression run in CI gated on competency stage
- Operator-facing "what has Max learned?" summary for PulseForge admins

# SPEC-132 — Specialist Execution Contract (SEC)

**Status:** Implemented  
**Depends on:** SPEC-118 (Acquisition Mission Orchestration), SPEC-130 (Mission Planning Engine), SPEC-131 (Transactional Mission Execution), SPEC-098 (Specialist Delegation)  
**ADR:** (pending)

## Purpose

Define a universal execution contract that every specialist (Scout, Paige, Vera, Rex, Emmett, etc.) must implement.

- The **Mission Planning Engine** decides what needs to happen.
- The **Transactional Execution Engine** decides when it executes.
- The **Specialist Execution Contract** defines how every specialist behaves.

No specialist invents its own lifecycle.

## Philosophy

**Specialists receive intent. Specialists do not interpret intent.**

Today Scout, Paige, and future specialists can evolve independently. Eventually they all have different inputs, outputs, errors, confidence models, and evidence formats. That becomes impossible to maintain.

Every specialist implements exactly one interface:

```
Mission Plan
      ↓
Execution Context
      ↓
Specialist
      ↓
Execution Result
```

No exceptions.

## Inputs

Every specialist receives:

| Field | Source |
|---|---|
| Mission Plan | Locked structured mission from SPEC-130 |
| Execution Context | Stage, status, missionId, tenantId |
| Workspace Context | Shared mission context (SPEC-118) |
| Blueprint Context | Business/blueprint snapshot — injected, never fetched by specialist |
| Evidence Policy | From mission plan |
| Memory Context | Mission observations |
| Operator Preferences | Operator-scoped settings |
| Transaction ID | TME transaction boundary |

**Forbidden:**

- No specialist parses workspace messages.
- No specialist retrieves Blueprint directly.
- No specialist determines mission type.

Implementation: `buildExecutionInput()` in `packages/acquisition-mission/SpecialistExecutionContract.js`.

Per-specialist structured inputs remain in `SpecialistInputs.js` (SPEC-130).

## Outputs

Every specialist returns exactly one object: **Execution Result**.

```javascript
{
  status: 'SUCCESS' | 'PARTIAL' | 'BLOCKED' | 'FAILED',
  confidence: { overall, evidence, fit, completeness },
  evidence: [{ source, confidence, timestamp, provenance, ... }],
  contributions: { /* specialist output contract payload */ },
  recommendations: [{ tier: 'required'|'suggested'|'optional', text, reason }],
  unknowns: [{ unknown, reason }],
  nextActions: [{ kind, label, reason, payload }],
  audit: { specialist, transactionId, executedAt, durationMs },
  explainability: {
    whyRecommended,
    whyNotRecommended,
    evidenceConfidenceChanges,
    remainsUnknown,
  },
}
```

Never arbitrary status strings. Never a bare floating-point confidence.

## Error Contract

Specialists never throw user-facing errors. Instead they return:

```javascript
{
  status: 'BLOCKED',
  blocked: {
    reason: '...',
    requiredPrecondition: '...',
    recommendedAction: '...',
  },
}
```

The Transaction Engine decides whether to rollback. `executeSpecialist()` wraps thrown exceptions into `FAILED` or `BLOCKED` results.

## Explainability

Every specialist automatically answers:

1. Why did I recommend this?
2. Why didn't I recommend that?
3. What evidence changed my confidence?
4. What remains unknown?

Provided via `explainability` on every Execution Result.

## Determinism

Specialists are forbidden from modifying:

- Mission
- Blueprint
- Workspace
- Memory

Only **contributions**. The Transaction Engine commits changes.

## Governance

Every specialist must satisfy:

| Contract | Validator |
|---|---|
| Input contract | `buildExecutionInput()` + `SpecialistInputs.js` |
| Output contract | `validateExecutionResult()` + `Contracts.js` |
| Audit contract | Required `audit.transactionId` |
| Confidence contract | Structured dimensions, 0–1 |
| Evidence contract | source + timestamp + provenance on every claim |

Otherwise execution fails validation and TME rolls back.

## Validation

Before commit, the Execution Engine validates via `assertExecutionResult()`:

- Required fields present
- Evidence format with provenance
- Confidence structure (not a bare float)
- Contribution schema per specialist
- Tiered recommendations
- Explainability block

If validation fails: rollback (SPEC-131).

## Integration

| Component | Role |
|---|---|
| `SpecialistExecutionContract.js` | SEC types, builders, validators, legacy mappers |
| `SpecialistInputs.js` | Per-specialist structured input (SPEC-130) |
| `Contracts.js` | Per-specialist contribution key contracts (SPEC-118) |
| `TransactionalExecution.js` | `assertExecutionResult()` hook before commit |
| `AmoOperatorApproval.js` | Discovery stage validates SEC envelope on Scout output |

## Specialist Registry

| Specialist | Input | Contribution keys |
|---|---|---|
| Scout | segment, geography, evidence policy | companies, prospects, buyingSignals, evidence |
| Paige | audience, campaign goal, tone | messaging, variants, subjects, hypotheses |
| Emmett | audience, deliverability policy | capacity, queue, sendRecommendations |
| Vera | market, companies, review policy | reviews, responses, draftResponses |
| Rex | mission, KPIs, progress | report, metrics, insights, recommendations |

## Legacy Bridge

Pre-SEC specialist outputs are normalized via `fromLegacyOutput()` and `fromScoutLegacyOutput()` so existing Scout/Paige agents can adopt SEC incrementally without rewriting business logic.

## Files

- `packages/acquisition-mission/SpecialistExecutionContract.js` — SEC implementation
- `packages/acquisition-mission/tests/spec132.test.js` — contract tests
- `packages/acquisition-mission/SpecialistInputs.js` — extended with `emmettInput`
- `packages/acquisition-mission/Contracts.js` — Vera/Rex output contracts
- `packages/acquisition-mission/types.js` — Vera/Rex specialist constants

## Note on SPEC-132 numbering

An earlier in-repo use of "SPEC-132" referred to **Acquisition Objective Precedence** in `WorkspaceOwnershipResolver.js`. This spec document defines **Specialist Execution Contract** as the canonical SEC. The workspace precedence behavior retains its implementation; SEC is the execution-layer contract numbered per product spec backlog.

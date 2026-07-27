# SPEC-005 — Policy & Decision Engine

| Field | Value |
|---|---|
| **Status** | Done |
| **Target Version** | v0.9.1 |
| **Priority** | Critical |
| **Owner** | TBD |
| **Created** | 2026-07-26 |
| **Completed** | 2026-07-26 |

## Objective

Introduce a deterministic policy engine that evaluates every proposed action before it reaches an operator or an autonomous workflow.

This becomes Pulseforge's safety layer.

## Philosophy

Reasoning determines: **What should happen?**

Policy determines: **What is allowed to happen?**

Those are different responsibilities.

## Vision References

- `docs/vision/Intelligence_Architecture.md`
- [SPEC-002](SPEC-002_Max_Reasoning_Engine.md)
- [SPEC-003](SPEC-003_Temporal_Intelligence_Memory.md)
- [SPEC-004](SPEC-004_Max_Briefing_Engine.md)
- [ADR-002](../adr/ADR-002_Explainable_AI.md)
- [ADR-003](../adr/ADR-003_Human_Approval.md)

## Problem

Once Max is wired into production, every feature asks: Can this recommendation be shown? Create a task? Send an email? Schedule a follow-up? Trigger automation? Without an explicit policy layer, those answers would be hardcoded, inconsistent, or silent.

## Scope

- Package `packages/max/policy/` — engine, rules, evaluation, approvals helpers, audit, tests
- Single entry: `policy.evaluate({ tenantId, recommendation, context })`
- Rule Registry (modular; no hardcoded branching)
- Seven initial rules: Confidence, Contradiction, Tenant Policy, Risk, Cooldown, Contact, Evidence Freshness
- Data-driven per-tenant configuration
- Immutable audit trail + explainability chain
- `max.decide()` on the library runtime

## Out of Scope

- Automatic execution
- UI / human approval screens
- Notifications
- Learning or adaptive policies

The engine should only **evaluate**.

## Dependencies

- ✅ Knowledge
- ✅ Reasoning (SPEC-002)
- ✅ Memory (SPEC-003)
- ✅ Briefing (SPEC-004)

## Architecture

```text
Knowledge → Reasoning → Memory → Briefing → Policy Engine → Operator / Future Automation
```

```js
const decision = await policy.evaluate({
  tenantId,
  recommendation,
  context,
});
```

Returns:

```text
PolicyDecision {
  allowed
  requiresApproval
  blocked
  severity
  reason
  matchedRules
  audit
}
```

### Rule Registry

```text
Policy Engine → Rule Registry → Execute Rules (priority order) → Decision
```

### Explainability

```text
Recommendation → Policy Decision → Matched Rules → Final Outcome
```

## Data Model

### Tenant policy (example)

```json
{
  "minimumConfidence": 0.75,
  "maximumRisk": 0.40,
  "approvalRequired": ["email", "linkedin"],
  "blockedDays": ["Sunday"]
}
```

No recompilation to change policy.

### PolicyAudit

`timestamp`, `recommendationId`, `decision`, `matchedRules`, `operator` — append-only, frozen records.

## Implementation Plan

1. PolicyTypes + RuleInterface + RuleRegistry
2. TenantPolicyStore (data-driven)
3. Seven initial rules
4. DecisionAggregator + PolicyAuditLog + ApprovalHelpers
5. PolicyEngine + runtime `decide()`
6. Tests + docs / release v0.9.1

## Migration Strategy

- Additive library under `packages/max/policy`
- Existing reasoning / memory / briefing unchanged
- Agents/server remain unwired

## Testing

```bash
npm run test:max
```

Covers: individual rules, rule ordering, multi-rule evaluation, approval/block scenarios, tenant configuration, audit generation, determinism.

## Acceptance Criteria

- [x] PolicyEngine implemented
- [x] Rule registry operational
- [x] Seven initial rules implemented
- [x] Tenant-specific policy supported
- [x] Structured audit trail generated
- [x] Deterministic evaluations
- [x] Runtime remains unwired
- [x] Existing reasoning unchanged

## Future Work

- Wire Max agent / outbox to `decide()` before any side effect (shadow-first)
- Operator approval screens consuming `approvalTicket`
- Persistent Postgres audit store
- Additional channel/volume rules per client

## Definition of Done

By completion of v0.9.1, every recommendation produced by Max can be deterministically evaluated against explicit, configurable business policies. The result is an explainable decision—allow, warn, require approval, or block—backed by an immutable audit trail. This creates the final control layer needed before integrating Max into the live product.

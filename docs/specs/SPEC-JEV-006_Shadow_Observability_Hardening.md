# SPEC-JEV-006 — Shadow Observability Hardening

Status: implemented. **Not a deployment spec.** JEV remains shadow-only; active routing stays disabled.

## Objective

Improve auditability of Jev shadow evaluations, AO comparability, pending-decision guard evidence, review reporting, and documented (disabled) deploy gates—without changing production routing.

## Scope

- Enriched `decision_shadow_events` columns (no raw operator message text).
- `decision_shadow_evidence` for `PENDING_DECISION_CAPTURE_GUARDED` and `DECISION_SHADOW_WARNING`.
- `observedRoute` coverage for AO ask/respond payloads.
- `npm run decision:review` enhanced summary and `--warnings`.
- `packages/decision-service/deployGates.js` constants (`ACTIVE_JEV_ROUTING_ENABLED = false`).

## Non-goals

- Enabling Jev active routing.
- Relaxing existing JEV tests or pending-decision production behavior.

## Migration

Apply `migrations/2026-09-30-jev-006-shadow-observability.sql` after SPEC-JEV-002.

## Review

```sh
npm run decision:review -- --limit 500 --json
npm run decision:review -- --warnings
```

## Deploy gates (documentation + code constants only)

See `packages/decision-service/deployGates.js`. Promotion requires human change of `ACTIVE_JEV_ROUTING_ENABLED` plus all gate predicates passing on production Postgres evidence.

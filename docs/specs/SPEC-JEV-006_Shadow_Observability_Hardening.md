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

### SPEC-PF-ACCEL acceptance hardening

Acceptance is a **read-only snapshot assessment**, not permission to expand
routing authority. `ACTIVE_JEV_ROUTING_ENABLED` remains false; the service also
rejects enabling it. There is no implemented active promotion path. Passing
analytics can only return `read_only_candidate`, never `broad_candidate`.

The overall threshold is at least **200 comparable decisions**. The existing
**200 workspace** and **100 AO ask/respond combined** thresholds remain separate,
additional safeguards. They are not interchangeable denominators: satisfying all
three requires at least 300 source-disjoint comparable decisions. Source counts
must honor `route_comparable` just like the overall count.

Errors or timeouts must affect **strictly less than 5%** of all evaluated-window
rows. Count the union of affected events; a timeout recorded as an error must not
be counted twice. Exactly 5% fails. Counts must be explicit nonnegative integers;
missing counts are not zero. Inspection/status match rate must be within [0.9, 1].

Zero dangerous mismatches must be established across a **complete seven-day
window**, including every `dangerous_*` class, not only approval-versus-
clarification. Pending-decision guard events must be verified; the presence of
one event with a matching name does not establish guard correctness.

#### Evidence still required before any production readiness claim

`decision:review` reads a bounded latest-row sample (maximum 500, optionally
filtered). Its guard-event count comes from a separate 30-day query. Neither is a
complete, same-scope seven-day acceptance record. The report now declares
`scope.window_complete: false` and leaves verified safety counts unknown. Even a
large clean sample therefore stays `shadow_only`. No new production collector,
queries, migrations, routing changes or permissions are introduced by this patch.

A future audited collector/operator review must supply the following evidence
from production Postgres for one explicitly identified tenant/population and
one common seven-day window:

- Complete event coverage, including failures/timeouts and every dangerous
  mismatch class; no latest-row limit, mismatch-only filter or silently omitted
  source. Establish persistence/drop coverage independently: merely querying all
  persisted rows does not prove every decision was captured.
- Exact total/comparable counts, source-disjoint workspace/AO counts, error count,
  timeout count and error-or-timeout union count.
- Explicit zero counts for both approval-versus-clarification and all dangerous
  mismatch classes. Unknown/unclassified safety evidence needs review, not an
  assumed zero.
- Same-tenant/window pending-decision guard receipts tied to their decisions,
  verifying deterministic blocking and expected pending-state behavior. Every
  counted guard receipt must be verified; a partially verified count fails. Review
  failed/missing receipts and test outcomes before attesting verification complete.
- Inspection/status comparable sample and match rate, with provenance sufficient
  to reproduce the snapshot. Review the snapshot's age before relying on it;
  this evaluator does not attest freshness or authenticate evidence.

The pure evaluator's snapshot contract is `scope.filter: 'all'`,
`scope.summary_basis: 'complete acceptance window'`, explicit
`scope.window_complete: true`, and ISO `window_start`/`window_end` exactly seven
UTC days apart (no future end). Summary fields include `comparable`, `total`,
`errors`, `timeouts`, `errors_or_timeouts`, `dangerous_mismatches_7d`,
`dangerous_approval_clarification_7d`, `pending_guard_evidence_rows`,
`pending_guard_verified_rows`, `pending_guard_verification_complete`,
`inspection_status_match_rate` and `by_source` counts. Both AO sources must have
explicit counts, including zero. Do not manually label a diagnostic sample
complete to make the gate pass. Unit-test snapshots are synthetic and are not
production acceptance evidence.

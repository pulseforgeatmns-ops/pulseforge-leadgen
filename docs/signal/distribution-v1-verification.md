# Distribution V1 verification — 2026-10-14

## Result

Local implementation complete and independently reviewed. Publication, merge,
deployment, provider probes, migrations in production and live messages remain
unperformed and separately approval-gated.

Final relevant gate: **201 passed, 0 failed, 0 skipped**, 18 suites.
Command:
    node --test test/signalDistribution*.test.js packages/signal-v1/tests/*.test.js test/globalProductionStartupSmoke.test.js test/ciPlanPr.test.js

Actual disposable PostgreSQL 17.11 tests covered concurrent registration/claiming,
durable reference and once-only milestone, restart state and ambiguity, scoped
reads, test isolation, capacity admissions, shared request quota and independent
channel delivery. No production connection was used. Both new PostgreSQL gates
passed without skipping; existing Signal PostgreSQL tests also passed.

Executed DOM/VM tests covered cancellation without writes, explicitly confirmed
earlier reference timestamps, duplicate in-flight saves, namespace and selected
monitor changes while requests were pending, failed-save retry, saved time labels,
and read-only refresh. Security tests cover role/session tenant, denied overrides
and impersonation, trusted origin/JSON guard, explicit reference confirmation and
TEST never sending.

Syntax and whitespace checks passed. New runtime, routes, UI, migration and tests
select Signal PR CI, and the standard Signal test command includes distribution.

## Visual limitation

Chromium could not start in this environment: process-singleton Unix socket
creation was denied before page load; an escalation attempt failed environment
mount setup. No visual verification or screenshots are claimed. No further
bypass attempts were made. A loopback-only fixture harness is saved at
scripts/signalDistributionBrowserQa.js for a supported browser environment.
The browser harness is separate from mandatory Node/PG CI gates.

## Independent review changes

Review caught and corrected zero-price treatment, stale-error clearing,
missingness bypassing critical cooldown, canonical critical-cause names,
per-process-clock quota risk, default capacity, durable backoff, stale/expired
delivery claims, milestone suppression by risk cooldown, duplicate/stale UI saves,
and misleading historical-MFE wording.

Reference entry accepts an explicit valid past timestamp, including before monitor
creation. MFE and once-only milestone detection only use samples processed after
reference.savedAt. Previously stored observations are not recomputed; recordedAt
and savedAt are shown separately.

## Remaining rollout checks

- Verify one-token first-admission selection matches tonight's intended monitor.
  Other distinct CAs are auditable capacity deferrals, never silent replacements.
- Approve additive schemas and feature activation; all flags default off.
- Validate actual public-provider fields and source lag. Source timestamps and
  wallet/developer coverage remain UNKNOWN.
- Verify all GeckoTerminal processes install the same durable budget. Six total
  attempts/minute can defer legacy 15s/30s research capture; no silent substitution.
- Approve each recipient/channel and its existing provider charges. SMS bodies
  max300 GSM7 characters (up to two segments); volume and actual rates vary.
- Confirm live recipient/provider acceptance and handset receipt separately.
  This candidate deliberately contains no live test-send bypass.
- Run browser visual verification in a supported environment before asserting
  visually verified mobile usability.

See distribution-v1-rollout.md for thresholds, rate limits, 24h TTL and activation.

### Activation-window safety extension
Explicit timezone windows (maximum24h), immutable restart configuration, DB-clock
admission/end fencing, prewindow caller rejection and durable SMS authorization
counts were added after the original green draft. Real PostgreSQL tests cover
concurrent reservations (8 contenders, cap5), restart/no refunds, skewed app clocks,
claim-before-end/authorize-after-end denial and no legacy-window sends. Provider
unit tests cover expiry between DB permission and invocation; existing consumer
pacing bypasses the distribution budget outside the window. Browser visual QA
remains unexecuted; DOM lifecycle tests additionally cover DB-expired status with
a skewed browser clock. Final exact-head CI is required after this extension.

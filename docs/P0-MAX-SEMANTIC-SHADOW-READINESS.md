# P0-MAX-REASONING-001 PR 2 — Semantic-shadow readiness and activation checklist

Status: **implemented and tested on a branch. Not merged, not deployed, not enabled. No production migration has been run.**
The observer stays default-off. Nothing here enables active semantic routing; `activeRouting` is hard-coded `false` in readiness.

## 1. What changed

| Area | Before | Now |
|---|---|---|
| Interpreter model | Three implicit defaults: producer `claude-haiku-5-5`; observer and eval `MAX_WORKSPACE_MODEL` → `claude-sonnet-4-6` | One explicit source, `MAX_INTERPRETATION_MODEL` (`packages/max/interpretation/interpreterModel.js`). No fallback, no built-in default. The producer refuses to construct without a model. An enabled observer without a model makes **no call and writes no row**, reporting `interpreter_model_not_configured` in observer health and readiness (`semanticShadow.model`). |
| Pending decision | `snapshot()` hard-coded `pendingDecisionId: null`, so every model `authorization` frame was repaired to `ambiguous` and authorization interpretation could not be measured | At the existing production hook where JEV snapshots the resolved mission (`captureMission`), the observer adds the mission's pending decision as an opaque candidate `pd:<missionId>:<kind>`. Only the kind is kept, never the prompt or package content. A mission from another tenant, or with no tenant, is ignored. The observer still has no route, approval or execution callback, and frames keep `authority.grantsAuthority = false`. |
| Truthful reporting | A degraded call's lexical fallback frame was compared with the legacy route and could be counted as agreement. The served model and truncation were not recorded. | Rows carry `frameSource` (`model` / `degraded_fallback`), `failure`, `stopReason`, `truncated`, `servedModel`, `modelMismatch`, `pendingDecisionBound` and `pendingDecisionKind`. A degraded frame's `disagreement` is `null` (incomparable). Cost is priced from the served model. Observer health records a degraded call as a failure (`model_<reason>`). |
| Retention | Documented ("governed maintenance deletes > 90 days"), not implemented | `shadowRetention.js` with `scripts/maxInterpretationShadowPurge.js` (dry run by default, `--apply` deletes). Batched, cutoff by `observed_at` only. |
| Review | turns / disagreements / degraded / p95 | Adds incomparable, truncated, model_mismatch and pending_decision_bound counts |
| Rollback | None | `migrations/2026-11-09-max-interpretation-shadow.rollback.sql` |

`WorkspaceEngine`: when the observer is off, `capture()` returns `null` and the JEV shadow object is passed to `_askProduction` **unchanged** (tested). When it is on, a tee forwards `captureMission` to JEV first, unchanged, then to the observer. The call site was already wrapped in `try/catch`.

## 2. Verification performed (disposable PostgreSQL 16, no real database)

`packages/max/interpretation/tests/shadowReadinessPostgres.postgres.cjs` (6 subtests) plus the existing `shadowPostgres.postgres.cjs`:

- **Migration.** Replayed twice. Columns, types, NOT NULL, the `(tenant_id, observed_at DESC)` index, and the `shadowOnly` CHECK (rejects `false`).
- **Persistence and privacy.** Five observations across two tenants (model ok, HTTP 500, truncation, bound authorization). No operator text, candidate label, pending prompt or e-mail address is persisted. `grantsAuthority` is false on every row.
- **Tenant isolation.** `review()` returns only the requested tenant's counts and rejects an injection-shaped tenant id. An unknown tenant returns nothing.
- **Retention.** Only rows older than 90 days are deleted, for every tenant, in batches. An 89-day row survives. Invalid retention values are refused.
- **Failure handling.** A database with no table: `observe()` does not throw, the failure is counted, and health shows `observer_failed`.
- **Rollback.** Drops the table. The rollback is replay-safe, and the migration re-applies cleanly.

Hermetic: `shadowReadiness.test.js` (15) covers the model resolver, no-call-when-unconfigured, the exact configured model sent through the metered client, pending-decision sanitization and tenant rejection, bound-vs-unbound authorization, degraded / truncated / mismatched / snapshot reporting, and the real `WorkspaceEngine` at the **plan-approval and discovery-approval gates**. At those gates the observer sees the bound decision and nominates approval, while the execution audit stays empty, the decision stays pending, the mission, events and contributions are byte-identical, and the production answer is identical to a run with the observer off. Mutation checks: removing the tee fails 2 tests; restoring the `null` pending decision fails 4.

Regression: JEV `npm run test:decision` 115/115 and `decisionShadowPostgres` pass. Authorization / pending-decision / continuity suites 515/515. `test:max:semantic` 9 pass, 1 todo (unchanged). `test:max:interpretation` 54/54. Max unit lane 2540/2541, 1 todo, 0 fail. Lexical baseline unchanged (47.7%).

## 3. Activation checklist (each step needs its own production approval)

**A. Preconditions**
1. PR reviewed and merged. Deploy completed. Confirm `GET /api/v1/max/operational-readiness` (admin/manager) reports `semanticShadow.configured:false` and `activeRouting:false`.
2. Revalidate the production baseline (JEV schema ready, no observer failures, outbound in maintenance). Do not rely on the 2026-11-18 snapshot.
3. Approve the interpreter model and a spend ceiling. The model must exist in `utils/apiPricing.js`, or every observation's cost is recorded as `null` (unknown). `claude-haiku-5-5` is **not** priced there today.

**B. Migration (governed operator workflow)**
1. Back up or snapshot per the normal migration procedure.
2. Apply `migrations/2026-11-09-max-interpretation-shadow.sql` (additive, idempotent, single transaction, no startup DDL).
3. Verify: `SELECT to_regclass('public.max_interpretation_shadow')` is non-null, and readiness `semanticShadow.schemaReady:true` with `persistence.rows:0`.
4. Verify the JEV schema is still ready (`jev.schema.ready:true`). The migration does not touch JEV tables.

**C. Enable (shadow only)**
1. Set `MAX_INTERPRETATION_MODEL=<approved model>` **first**, then `MAX_INTERPRETATION_SHADOW=true`. `ANTHROPIC_API_KEY`, `DATABASE_URL` and the shared API spend controls already exist; nothing new is introduced.
2. Never set any frame-routing or two-key authority flag. None is consumed by this code.
3. Smoke check: one operator turn on a non-production tenant. Expect one row with `frameSource:'model'`, `servedModel` equal to the configured model, and `modelMismatch:false`.

**D. Monitoring (daily during the first two weeks)**
- `node scripts/maxInterpretationShadowReview.js <tenant>` per active tenant. Watch degraded / turns ≤ 2%, `truncated` = 0, `model_mismatch` = 0, and p95 latency ≤ 1500 ms. Disagreements are triage signals, not semantic judgements.
- Readiness `observerHealth.observers.semantic_shadow`: `failed`, `dropped` and `lastFailureReason` (`model_timeout`, `model_http_*`, `model_stop_reason_max_tokens`, `observer_failed`, `interpreter_model_not_configured`). Health is per process and not durable.
- API spend for agent `max-interpretation-shadow` in the shared spend ledger.
- Any `truncated > 0`: disable and fix the producer's response cap with a regression test before re-enabling.

**E. Retention**
- Schedule `node scripts/maxInterpretationShadowPurge.js` (dry run) then `--apply` daily in the governed maintenance workflow **before** enabling writes. Retention is 90 days. The review query already ignores older rows.

**F. Rollback**
1. Immediate: unset `MAX_INTERPRETATION_SHADOW` (or set it to anything but `true`). No calls, no writes, no captures. Production routing is unaffected either way.
2. Data: run the purge, or, to remove all observations, apply `migrations/2026-11-09-max-interpretation-shadow.rollback.sql` (permanently deletes; nothing else reads the table).
3. Code: reverting the PR restores the previous observer. The table and rows are compatible (only additive JSON fields).

**G. Authorization requirements (unchanged and not bypassed)**
The observer cannot call `routeExecutionRequest`, the Transactional Mission Engine, pending-decision resolution, `AuthorizationConstraints` or outbound control. A frame's `executionCandidate` is informational only. Active routing remains out of scope and needs the §10 promotion gates plus separate approval.

## 4. Known limits

- The pending decision is captured only on turns where production resolves a mission (`analyzeOperatorIntent`). Turns that never resolve one keep `pendingDecisionId:null`, and an authorization frame there is still repaired to `ambiguous`. This is a safe direction, and `pendingDecisionBound` makes it visible.
- The execution-approval gate's binding to the **presented package revision** is enforced by production, not reflected in the candidate id. The observer cannot authorize anything, so this only affects measurement granularity.
- Retention is a scheduled job, not a database TTL.
- The pre-existing `test/maxOperationalReadiness.test.js` assertion `implementedInDeployedCode: false` fails on `main` (stale against current code). It was left unchanged here.

## 5. Relation to PR 1 (#1131)

Independent: this branch is cut from `main` and does not require PR 1. Overlaps, verified with a trial merge of both branches:
- `ModelFrameProducer.js`: both add the same `servedModel` telemetry lines (identical text, so it merges cleanly).
- `scripts/maxInterpretationEval.js`: PR 2 makes the existing `--model` path use `--model-id` or `MAX_INTERPRETATION_MODEL` (no default). PR 1 rewrites the file and requires `--model-id`. If both merge, keep PR 1's version; it is the stricter of the two.
- `docs/P0-MAX-SHADOW-INTERPRETER.md`: separate paragraphs.

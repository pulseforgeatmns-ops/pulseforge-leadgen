# SPEC-CI-LEAN-001 — PulseForge Lean PR CI Audit

**Date:** 2026-10-09  
**Status:** Implemented on branch `cursor/ci-lean-001-27a2` (pending review)

---

## 1. Current workflow topology (before)

### Merge-blocking on every pull request (no path filter)

| Workflow | Domain | Typical steps |
|----------|--------|----------------|
| `paige-social-safety.yml` | Paige / content outcome | Postgres setup, `npm run test:paige` |
| `revenue-postgres.yml` | Revenue + AO routing + harness | Postgres, 6 npm test stages, deployment gate script |
| `anchor-governed-outbound-tests.yml` | Anchor / Babrun governed outbound | Postgres, `test:anchor-outbound` + 15+ mailbox/scout tests |
| `decision-shadow.yml` | Max decision shadow | Postgres, `test:decision` + `test:decision:postgres` |

### Path-filtered on pull request

| Workflow | Trigger |
|----------|---------|
| `signal-v1-core.yml` | Signal paths, `package*.json`, `server.js` |
| `walkthrough-notification-safety.yml` | Walkthrough paths |
| `spec245-evidence.yml` | SPEC-245 / CIE paths |
| `anchor-canonical-outbound.yml` | Canonical outbound scripts (+ `routes/cron.js`) |

### Not on PR critical path

Scheduled / manual / post-deploy: `penny-anchor-daily`, `phase3f-disposable-postgres-release-gate`, `spec252/253/254` acceptance, enrichment crons, etc.

**Problem:** Four heavy Postgres workflows ran on **every** PR regardless of diff. Signal was the only domain with path awareness; it still shared the wall with four unrelated suites.

---

## 2. Thirty-day evidence (~80 PR runs per always-on workflow)

Sample: last ~500 workflow runs, PR events only (Oct 2026 window).

| Check | PR runs | Pass rate | Median runtime | p95 (prior sample) | Notes |
|-------|---------|-----------|----------------|---------------------|-------|
| Revenue PostgreSQL Integration | 80 | 82.5% | **65s** | 83s | Largest PR cost; AO + revenue + harness |
| Anchor governed outbound safety | 80 | 86.3% | **49s** | 64s | Second-largest; Postgres + broad mailbox matrix |
| Decision shadow review | 80 | 90.0% | **36s** | 69s | Max shadow packages + Postgres |
| Paige governed social safety | 80 | 86.3% | **36s** | 49s | Postgres + Paige/content learning |
| Signal V1 Core | 9 | 66.7% | **58s** | 79s | Path-filtered — rarely skipped on Signal PRs |

**Approximate compute per PR (before):** median **~186s** of runner time summed across the four always-on jobs, plus **~58s** when Signal paths matched → **~244s** for Signal-shaped PRs. Wall-clock ≈ **max(65,49,36,36,58) ≈ 65–70s** (parallel), but billed minutes sum all jobs.

**Flaky / unrelated failure patterns:**

- **~3–4s failures** on several PRs (e.g. Scout inventory branches): workflow startup/cancel or infrastructure, not assertion failures in product tests.
- **Paige failures on non-Paige PRs** (e.g. `fix/max-spreadsheet-reliability`): failing social/content tests on spreadsheet CRM changes — high **unrelated failure** signal.
- **Signal PR #899** (`cursor/signal-live-rollout-alert-5bb5`): triggered all four always-on workflows plus Signal — only Signal + global startup were justified by files changed.

---

## 3. Domain ownership map

Canonical map: [`.github/ci/domains.json`](../../.github/ci/domains.json)

| Domain | Representative paths |
|--------|----------------------|
| **Global** | `server.js`, `db.js`, `middleware/`, CI selector |
| **Signal V1** | `packages/signal-v1/**`, `services/signalOperator/**`, `services/telegramCallerFeed/**`, `routes/signalV1.js`, `deployment/signal-v1/**` |
| **Max** | `packages/max/**`, `maxAgent.js`, `routes/max*.js`, `test/max*.test.js` |
| **Paige** | `paigeAgent.js`, `routes/paigeSocial.js`, `utils/publishPipeline.js`, `test/paige*`, content outcome/learning |
| **AO / CRM** | `routes/ao*.js`, `utils/ao*Schema.js`, `test/ao*`, AO tests inside revenue suite |
| **Anchor / Babrun outbound** | `services/governedOutbound*`, `anchorDailyOutboundCron.js`, `test/anchorDailyOutbound*`, `test/governedOutbound*` |
| **Studio Substral** | `sites/studio-substral/**`, `routes/substralAssessment.js`, `test/substral*`, `test/specSubstral*` |
| **Revenue / Postgres** | `routes/revenue.js`, harness + revenue + AO postgres tests |
| **Decision shadow** | `test/decision*`, `packages/max/workspace/tests/specJev*` |
| **Walkthrough** | `routes/walkthrough.js`, `lib/walkthrough*`, notification tests |
| **SPEC-245 evidence** | `lib/canonicalSemanticWrite.js`, spec245/224/223 tests |
| **Anchor canonical outbound** | `scripts/*AnchorCanonicalOutbound*`, recovery test, `routes/cron.js` |

Shared dependency escalation (documented in `domains.json` → `escalationRules`): `package.json`, `server.js`, `db.js`, `migrations/`, `routes/cron.js`, `routes/api.js`, `.github/workflows/`, `packages/acquisition-mission/`.

---

## 4. Check classification

| Suite | Class | PR blocking (after) | Full regression |
|-------|-------|---------------------|-----------------|
| Global startup smoke | **A — GLOBAL CRITICAL** | Always | Yes |
| Signal V1 core | **B — DOMAIN CRITICAL** | Signal paths | Yes |
| Paige social safety | **B** | Paige/content paths | Yes |
| Anchor governed outbound | **B** | Outbound/mailbox paths | Yes |
| Decision shadow | **B** | Decision / shadow paths | Yes |
| Revenue PostgreSQL | **B** | Revenue/AO/migration paths | Yes |
| Walkthrough / SPEC-245 / canonical outbound / Substral / Max | **B** | Respective paths | Partial (Max, Substral in nightly matrix) |

**C — FULL REGRESSION:** Entire platform slices in `full-regression.yml` (push to `main`, nightly 06:00 UTC, manual).

**D — REDUNDANT (mitigated, not deleted):**

- `test:ci:application` is only `missionRouting.test.js` — kept inside **revenue-postgres** suite when AO/Max routing paths change; not run on Signal-only PRs. Overlap with `test:mission` is acceptable off PR path.
- `test/signalV1Startup.test.js` stays in Signal job only (not duplicated globally). **Global** adds `test/globalProductionStartupSmoke.test.js` (`node --check server.js` + `db` import) — single production parse boundary.

**E — FLAKY / ENV:** Short-lived workflow failures; no retry padding added. Fail-closed selector widens suites on planning errors.

---

## 5. Implementation summary

| Deliverable | Location |
|-------------|----------|
| Path-aware PR orchestrator | `.github/workflows/pr-ci-lean.yml` |
| Suite selector + fail-closed | `.github/scripts/ci-plan-pr.js` |
| Domain map | `.github/ci/domains.json` |
| CI summary | GitHub Step Summary from selector |
| Broad regression off PR path | `.github/workflows/full-regression.yml` |
| Selector tests | `test/ciPlanPr.test.js` |
| Global smoke | `test/globalProductionStartupSmoke.test.js` |

Legacy workflows (`paige-social-safety`, `revenue-postgres`, `anchor-governed-outbound-tests`, `decision-shadow`, `signal-v1-core`, etc.) → **`workflow_dispatch` only** to avoid duplicate PR runs. Manual reruns remain available.

---

## 6. PR-critical matrix (after)

| Change shape | Suites selected |
|--------------|-----------------|
| Signal-only (PR #899) | `global`, `signal-v1` |
| Paige-only | `global`, `paige-social` |
| AO routing only | `global`, `revenue-postgres` |
| `package.json` | `global` + 5 Postgres-heavy domains (see escalation rule) |
| `server.js` | `global`, `anchor-outbound`, `signal-v1`, `revenue-postgres`, `decision-shadow` |
| CI workflow edit | All suites (fail-safe validation) |
| Docs-only | `global` only |

---

## 7. PR #899 case study

**Title:** Signal V1 live rollout: protected feed auth and Front Runners operator alerts  
**Files:** 17 paths — all under `packages/signal-v1/`, `services/signalOperator/`, `services/telegramCallerFeed/`, `services/signalV1ShadowScheduler.js`, `scripts/signalOperatorTransportTest.js`, `deployment/signal-v1/`.

### Before (actual GitHub Actions)

| Workflow | Necessary? | Why |
|----------|------------|-----|
| Signal V1 Core | **Yes** | Direct domain coverage |
| Anchor governed outbound | **No** | No governed outbound / mailbox files |
| Decision shadow review | **No** | No Max shadow / decision files |
| Paige governed social | **No** | No Paige/content files |
| Revenue PostgreSQL | **No** | No revenue/AO/schema files |

### After CI-LEAN-001 (selector output)

```
Running: global, signal-v1
Skipped: paige-social, anchor-outbound, decision-shadow, revenue-postgres, max, …
```

**Estimated runtime:** ~75s summed runner time (global ~20s + Signal ~58s) vs **~244s** before for the same diff.

---

## 8. Before / after estimates

| Metric | Before (median Signal-shaped PR) | After (CI-LEAN-001) |
|--------|----------------------------------|---------------------|
| Merge-blocking jobs triggered | 5 | **2** |
| Unrelated domain jobs | 4 | **0** |
| Summed runner seconds (approx.) | ~244s | **~75s** |
| Summed runner minutes (billing proxy) | ~4.1 min | **~1.3 min** |
| Unrelated check exposure | 4 suites × 131 PRs/30d | Scoped — **~69%** reduction in always-on Postgres jobs for domain-scoped PRs (varies by diff) |
| Full platform regression | Ad hoc via every PR | **`full-regression.yml`** on `main` + nightly |

---

## 9. Invariant removal log

No merge-blocking tests were deleted in this change. Coverage moved off the PR path only where paths do not touch the domain:

| Invariant | Remaining coverage |
|-----------|-------------------|
| Paige publish/approval gates | `paige-social` job when Paige paths change; nightly regression |
| Revenue disposable harness | `revenue-postgres` job when revenue/AO/migration paths change; nightly |
| Governed outbound startup | `anchor-outbound` when outbound/server escalation; `governedOutboundStartup.test.js` unchanged |
| Decision shadow Postgres | `decision-shadow` job on shadow paths; nightly |
| Production deploy parse | **`globalProductionStartupSmoke`** on every PR |

---

## 10. Review checklist

- [ ] Confirm branch protection required checks updated to **`PR CI (SPEC-CI-LEAN-001)`** / gate job (repo admin).
- [ ] Review escalation rules for `package.json` / `server.js` — widen if missed cross-domain regressions appear.
- [ ] Compare first week of `full-regression.yml` failures against historical PR noise.

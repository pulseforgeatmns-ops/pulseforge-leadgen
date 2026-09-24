# AUDIT-WEB-001 — Live Cohort Validation Report

**Date:** 2026-09-24  
**PR:** [#707](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/707) (remains draft)  
**Cohort:** WEB-COHORT-002  
**Status:** **INCOMPLETE — discovery blocked in audit environment**

---

## Executive Summary

AUDIT-WEB-001 infrastructure was implemented (`scripts/runWebCohort002.js`, `scripts/lib/webCohort002.js`) to discover 25 real U.S. businesses via the **implemented Scout path** (`leadgen.searchGooglePlaces` + `leadgen.searchGoogle` additive) and run **live read-only audits** (HTTP/HTML, optional PageSpeed Insights, optional Puppeteer DOM).

**Execution in this Cloud Agent environment failed before discovery** because neither `GOOGLE_PLACES_KEY` nor `SERPAPI_KEY` is configured. Scout discovery returned zero candidates; WEB-COHORT-002 was not populated.

No outreach was attempted. No scoring weights were tuned. PR #707 remains draft.

---

## Implementation Delivered for Audit

| Artifact | Purpose |
|---|---|
| `scripts/lib/webCohort002.js` | Scout discovery rotation + live assessment pipeline |
| `scripts/lib/webCohortShared.js` | Shared cohort report, Max priority, false-positive audit |
| `scripts/runWebCohort002.js` | CLI (`--confirm-live`, optional `--apply`, `--skip-puppeteer`) |
| `leadgen.js` exports | `getSearchQueriesForTarget`, `searchGooglePlaces` for cohort Scout path |
| `pagespeedProvider.js` | Records **UNKNOWN** when PSI unavailable (no model substitution) |

### Discovery design (not cherry-picking bad sites)

Rotates 15 U.S. markets × ICP verticals (legal, accounting, dental, HVAC, etc.). Accepts candidates in Scout return order until 25 unique domains with websites. Records per-candidate `discovery` provenance (scout path, query, vertical, location, acceptance order).

### Live audit design

- No fixture businesses
- No fixture audit provider
- `skipPuppeteer: false` by default
- PageSpeed when `GOOGLE_API_KEY` or `GOOGLE_PLACES_KEY` present; otherwise UNKNOWN finding
- Existing scoring and economics models unchanged

---

## Execution Attempt

```bash
node scripts/runWebCohort002.js --confirm-live
```

**Result:**

```
Scout discovery unavailable: set GOOGLE_PLACES_KEY and/or SERPAPI_KEY for live cohort discovery
```

| Env var | Audit VM |
|---|---|
| `GOOGLE_PLACES_KEY` | **Not set** |
| `SERPAPI_KEY` | **Not set** |
| `GOOGLE_API_KEY` | **Not set** |
| `DATABASE_URL` | **Not set** |

Outbound HTTP verified working (live fetch to public sites succeeds). Scout Places/Serp discovery cannot run without credentials.

---

## WEB-COHORT-002 Results

**Not produced.** Target 25; discovered **0**.

Report placeholder: `artifacts/spec-web-001/cohort-report-002-live.json` (written only on successful/incomplete run — incomplete stub not committed with 25 rows).

---

## WEB-COHORT-001 vs WEB-COHORT-002 Comparison

| Dimension | WEB-COHORT-001 (fixture) | WEB-COHORT-002 (live, attempted) |
|---|---|---|
| Discovery | Hard-coded fixture businesses | Scout Places/Serp rotation (blocked) |
| Audit | Fixture audit profiles | Live HTTP/PSI/Puppeteer (not reached) |
| Distribution | DO_NOT_PURSUE 2, MONITOR 11, AUDIT 8, HIGH_VALUE 4 | N/A |
| Evidence | Synthetic MEASURED PSI scores | Would be live measurements |
| Buying signals | Injected `hiring_signal` flags | Would require live OBSERVED evidence only |

### Assumptions that could not be validated against live data

1. **Fixture PSI scores predict live mobile performance** — not testable without cohort execution.
2. **HIGH_VALUE rate (~16%) reflects real market** — not testable.
3. **Contactability from Scout discovery** — not testable without Places/Serp.
4. **Buying signals without injected flags** — not testable.
5. **Max deprioritization of high raw scores with weak economics** — partially validated in unit tests only, not live cohort.

**Do not correct assumptions during this audit** — listed for a follow-up spec after credentials are available.

---

## False-Positive Audit

**Not executable** on live cohort (0 candidates). Unit tests continue to cover scoring discrimination logic from SPEC-WEB-001.

---

## Tests (post-audit code changes)

```bash
npm run test:web-opportunity   # expected pass
npm run test:capabilities      # expected pass
```

Existing tenants unaffected — no changes to Anchor/Pulseforge/MSHI scoring paths.

---

## How to Complete WEB-COHORT-002 (operator action)

Run in an environment with Scout credentials (Railway production shell or local `.env`):

```bash
export GOOGLE_PLACES_KEY=...
export GOOGLE_API_KEY=...      # optional but recommended for PSI MEASURED evidence
export DATABASE_URL=...        # optional unless using --apply

node scripts/runWebCohort002.js --confirm-live
# persist assessments + events:
node scripts/runWebCohort002.js --confirm-live --apply
```

Output: `artifacts/spec-web-001/cohort-report-002-live.json`

---

## Verdict

# REQUIRES CORRECTION

### Blocking defects

1. **Audit environment missing Scout discovery credentials** — `GOOGLE_PLACES_KEY` and/or `SERPAPI_KEY` required to discover 25 real businesses via `leadgen.searchGooglePlaces` / `leadgen.searchGoogle`. Without these, WEB-COHORT-002 cannot be executed and AUDIT-WEB-001 acceptance criteria are unmet.

2. **WEB-COHORT-002 not executed** — No live cohort report with 25 real businesses, top-five live evidence, or live false-positive audit.

3. **DATABASE_URL absent in audit VM** — Maynard Web tenant resolution and persisted cohort artifacts require DB for `--apply` runs (non-blocking for read-only audit if credentials present, but production validation expects persistence).

### Recommended changes (separate from this audit — do not tune scoring)

1. Add `GOOGLE_PLACES_KEY`, `GOOGLE_API_KEY`, and `DATABASE_URL` to the Cloud Agent environment (or run cohort from Railway shell against production DB read-only + Places quota).

2. Re-run `node scripts/runWebCohort002.js --confirm-live --apply` and attach `cohort-report-002-live.json` to PR #707.

3. Re-evaluate AUDIT-WEB-001 verdict after live report exists; only then consider MERGE READY for the intelligence layer.

### Not blocking (informational)

- PageSpeed now correctly emits UNKNOWN when API key missing (audit-compliant).
- Cohort runner and Scout path wiring are implemented and ready.

---

## Hard Boundary Confirmation

- No emails, calls, forms, DMs, publication, paid acquisition, or outreach enabled
- Paige/Emmett not enabled for Maynard Web
- PR #707 remains **draft**

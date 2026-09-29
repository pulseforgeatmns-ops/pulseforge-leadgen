# SPEC-WEB-001 Implementation Report

## Architecture

### Reused PF components
- Multi-tenant `clients` registry (`utils/clientContext.js`, slug `maynard-web`)
- Acquisition Mission Orchestration (`services/acquisitionMission.js`, canonical lifecycle)
- Capability framework (`packages/capabilities/`) — new capability registered alongside existing built-ins
- Scout pipeline (`leadgen.js`) — post-save assessment hook for `scoring_profile='web_design'`
- Max digest (`maxAgent.js`) — web opportunity prioritization section
- Paige evidence bridge (`utils/paigeWebEvidenceContext.js`) — future copy only, no send
- Agent observability pattern — structured `website_opportunity_events`

### New components
- `packages/capabilities/websiteOpportunityIntelligence/` — reusable capability
- `services/websiteOpportunityPersistence.js` — assessments + events
- `services/webDesignScout.js` — Scout integration wrapper
- `utils/maynardWebTenant.js` — idempotent tenant + mission bootstrap
- `utils/webOpportunityMaxPrioritization.js` — Max ranking helper
- `scripts/lib/webCohort001*.js` + `scripts/runWebCohort001.js` — WEB-COHORT-001 dry run
- Migration `migrations/2026-09-24-spec-web-001-website-opportunity.sql`

## Tenant

- **Name:** Maynard Web (neutral working identity)
- **Slug:** `maynard-web` (resolved at runtime — **no hard-coded tenant id**)
- **Scoring profile:** `web_design`
- **Enabled agents:** `scout`, `max` only (no Emmett/outreach)
- **Mission objective:** Acquire one profitable website redesign client ≥ $2,500 contract value while minimizing operator acquisition time

## Capability

`website_opportunity_intelligence` v1.0.0

Pipeline: **DISCOVER → AUDIT → DIAGNOSE → SCORE → PRIORITIZE → ASSESS** (stops before OUTREACH)

Deterministic tooling:
- HTTP fetch + HTML parse (HTTPS, meta, viewport, alt text, conversion structure)
- Optional PageSpeed Insights API (`GOOGLE_API_KEY`)
- Optional Puppeteer DOM observation (CTA/nav/phone visibility) — skipped in cohort dry-run
- Fixture audit provider for reproducible validation cohort

## Evidence Model

Classes: `MEASURED`, `OBSERVED`, `INFERRED`, `UNKNOWN`

- MEASURED requires measurement payload (enforced in `evidence.js`)
- UNKNOWN cannot carry fabricated negative sales assertions
- Assessment artifacts retain `evidence_refs` traceable to findings
- Commercial diagnosis separated from deterministic audit output

## Scoring

Transparent 0–100 model (`scoring.js`):

| Component | Max |
|---|---|
| Website deficiency | 25 |
| Commercial value | 25 |
| Buying signals | 20 |
| Contactability | 15 |
| Project economics | 15 |

High deficiency alone triggers `deficiency_only_risk` and caps recommendation quality.

## Economics

`economics.js` — all values labeled **estimate**:
- Operator labor: **$50/hour**
- Capacity reference: **40 hours / rolling 30 days**
- Formula: `Contract - (Hours × $50) - Direct Costs = Contribution`
- Projects consuming ≥75% capacity at floor are penalized in prioritization

## Integrations

| Agent | Integration |
|---|---|
| Scout | `leadgen.js` + `services/webDesignScout.js` — discover, audit, persist assessment, map score to `icp_score` |
| Max | `listAssessmentsForClient` + `buildMaxWebOpportunityDigest` in daily digest |
| Paige | `buildPaigeWebEvidenceContext()` — supported findings only; prohibited-claim guard |

## Safety

WEB-COHORT-001 and capability execution are **read-only**:
- No email, calls, forms, publication, proposals, payments, or paid acquisition
- Cohort runner requires explicit flags; fixture mode avoids live network by default
- `enabled_agents` excludes Emmett and all outbound agents for Maynard Web

## Tests

`npm run test:web-opportunity` — 10 tests passing:
- Evidence integrity, scoring discrimination, economics arithmetic, safety, multi-tenant isolation patterns, cohort completion

## Cohort — WEB-COHORT-001 (fixture dry-run, n=25)

| Recommended action | Count |
|---|---|
| DO_NOT_PURSUE | 2 |
| MONITOR | 11 |
| AUDIT_WORTH_REVIEWING | 8 |
| HIGH_VALUE_WEBSITE_OPPORTUNITY | 4 |

## Top Five (Max priority, not raw score)

1. **Ridgeline Law Group** — HIGH_VALUE — strong deficiency + legal commercial value + hiring signal + contactability
2. **Craftsmen Builders Group** — HIGH_VALUE — multi-location home services, hiring, material deficiencies
3. **Stonebridge Architecture** — AUDIT_WORTH_REVIEWING — high-value vertical, strong deficiency, operator review warranted
4. **Prairie Legal Associates** — AUDIT_WORTH_REVIEWING — legal + moderate deficiency
5. **Blue Ridge Dental Care** — MONITOR — strong business, adequate site (valid false-positive guard)

## Problems / Limitations

- Live PageSpeed/Lighthouse requires `GOOGLE_API_KEY`; cohort uses fixtures for reproducibility
- Puppeteer DOM observation is optional and skipped in default cohort path
- Accessibility coverage is basic HTML heuristics + PSI when available — not a legal compliance determination
- Mission persistence depends on AMO tables being present in environment
- Vertical tuning for web-design ICP still requires operator review of cohort false-positive cases (11 flagged)

## Recommendation

**Not ready for authorized acquisition experiment yet.**

The validation cohort demonstrates scoring discrimination and conservative evidence handling, but operator review of the 25 fixture assessments (especially MONITOR vs AUDIT_WORTH_REVIEWING boundaries) should precede any separately authorized outreach pilot.

**Outreach remains disabled.** Do not enable Emmett or external acquisition without explicit operator approval after cohort review.

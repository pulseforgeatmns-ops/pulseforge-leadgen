# SPEC-WEB-001A Implementation Report

## Summary

Live WEB-COHORT-002 exposed evidence taxonomy, scoring collapse, buying-signal, economics, discovery, diagnosis, and Max prioritization defects. SPEC-WEB-001A corrects the website opportunity intelligence pipeline without tuning classification thresholds or enabling outreach.

## Corrections Delivered

| Area | Fix |
|---|---|
| Evidence taxonomy | `inference.js` builds bounded INFERRED findings from MEASURED/OBSERVED sources; `enforceEvidenceIntegrity` rejects verbatim duplicates |
| Deficiency scoring | Documented `DEFICIENCY_SCORE_RULES` in `scoring.js` — fetch time, PSI, a11y severity, sitemap/robots, conversion, viewport, HTTP errors |
| Buying signals | `buying_signal_research` gate; unscored UNKNOWN when research not performed |
| Contactability | Extended scoring for contact page, mailto, form observations from audit |
| Economics | Split `default_planning_economics` vs `prospect_specific_economics`; `economic_confidence` gates project_economics scoring and Max contribution weight |
| Discovery admission | `discoveryAdmission.js` rejects maps/search/directory domains, generic titles, duplicates |
| Cohort diversity | `webCohort003.js` stratified round-robin assembly across 15 market×vertical strata |
| Commercial diagnosis | `diagnosis_class`: HEALTHY_SITE, TARGETED_REMEDIATION, REDESIGN_CANDIDATE, INSUFFICIENT_EVIDENCE |
| Max prioritization | Weights deficiency, diagnosis, evidence quality; suppresses generic economics when confidence LOW/UNKNOWN |
| PageSpeed | PSI telemetry: `psi_attempted`, `psi_success`, `psi_failed`, `psi_unknown` with failure reasons |

## WEB-COHORT-003 Revalidation

Runner: `node scripts/runWebCohort003.js --confirm-live`

**Cloud Agent execution:** Blocked — `GOOGLE_PLACES_KEY` and `SERPAPI_KEY` not configured in audit VM. Same constraint as AUDIT-WEB-001 / WEB-COHORT-002.

Report path on success: `artifacts/spec-web-001/cohort-report-003-live.json`

## Tests

`test/specWeb001aCorrections.test.js` — 14 regression cases covering all acceptance gates except live cohort population (requires Scout credentials).

Existing capability and tenant regression suites pass.

## Outreach

No outreach authority granted. PR #707 remains draft until WEB-COHORT-003 is reviewed with live credentials.

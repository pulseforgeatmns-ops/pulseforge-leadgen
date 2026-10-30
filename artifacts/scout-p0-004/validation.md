# SPEC-SCOUT-P0-004 validation

Local regression result: **159 passed, 0 failed, 0 skipped** (9 suites).

Includes 15 new deterministic/evaluation/provider-boundary cases: positive/negative
facility classifications, country/state/town ambiguity, independent procurement,
physical identity preservation, historical-deferral review holds, deterministic
offline evaluation, exact-grant read-only export and eight restarted synthetic
provider/checkpoint passes covering eight approved categories and eight towns with
one allocated page per pass. External providers were denied throughout.

Additional regressions cover P0 replenishment, grant shape/permissions, research
parking, query checkpoints/continuation, tenant discovery memory, candidate
ticket dispatch, mission geography and CI lane selection.

The native PostgreSQL eight-cycle test is implemented and required in the PR
workflow, but **could not execute in this environment**: `initdb` and `pg_ctl`
are unavailable. Its local attempt failed at harness initialization before
application execution. Native PostgreSQL and PR CI acceptance remain pending;
this document does not claim green CI.

The actual-data retrospective covers 52/79 accessible observations. Full-cohort
acceptance remains pending the read-only raw export. Only synthetic fixture data
and aggregate findings are included in the repository package.

No production mutation, paid search/enrichment, website fetch, preparation or send
was performed. The proposed runtime path remains default off.

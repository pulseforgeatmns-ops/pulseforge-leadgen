# Scout P0-004: complete research-provider issue summary

Implementation and regression review only. No production activation, grant
replacement, cycle release, paid search, classifier change, website fetching,
candidate promotion or downstream activity is included.

## Problem

The operator research receipt retains the first 20 provider reports. A native
facility pass contains repeated identity workloads and empty website adapter
entries. Those can fill the sample before optional-adapter errors or real
failures occur. The diagnostic therefore cannot explain a completed research
cycle: `reportsTruncated=true`, `providerIssuesComplete=false`, blocker unknown.
Increasing the sample size only moves that boundary.

## Change

For research-only diagnostics, aggregate the complete available provider pass
separately from the existing 20-row detail sample. The summary exposes fixed
provider/status counts and fixed code/status/cause groups, never raw messages,
queries, contact information, URLs or provider identifiers outside the allowlist.
Counts describe observations, not unique HTTP calls, money spent or admissions.
Source adapter errors and execution errors can describe the same attempt.

The summary scans at most 1,000 reports and 10,000 error/query observations,
with at most 64 grouped issue rows. It reports observed totals, scanned and
omitted counts, malformed observations, unexplained failures and grouping
overflow. Missing, malformed or bounded-away observations never establish
complete diagnostics. Known denials, quota errors and provider failures stay
visible even if the rest of the projection is incomplete.

`reportsTruncated` continues to describe the 20-row sample.
`providerIssuesComplete` now describes the aggregate's coverage of the supplied
pass, and is also exposed as `providerIssueSummary.complete`. This does not
claim that a provider supplied every HTTP settlement or that an upstream
omission can be reconstructed. Historical receipts remain unchanged.

Completed research with only optional adapter unavailability may report that
issue as nonblocking only when the aggregate is complete. Coverage and target
limits remain explicit; unknown/mixed errors and unexplained failed reports
remain failures. An oversized receipt without another known cause reports
`diagnostic_incomplete`, not healthy. Ordinary Scout telemetry keeps its
existing semantics. All fields remain observational and cannot authorize work.

## Verification

Synthetic regressions place optional unavailability, denials, quota errors,
unknown failures, scope limits and query failures after the sample boundary.
Additional cases exceed error, query, report and group bounds, preserve source
objects, and assert that arbitrary private text does not escape. Provider
transports are denied. The native disposable-PostgreSQL facility journey must
record a complete aggregate and an explicit coverage limit, preserve the
one-page budget and unreleased-cycle refusal, and leave all protected recovery,
suppression, mission and outbound tables unchanged. Existing release/grant
regressions remain required. Missing PostgreSQL prerequisites must not be skipped.

No schema migration, production data rewrite or authority change is needed.
A future deployment changes the revision pin and requires the existing
separate production grant/release review before any new research execution.

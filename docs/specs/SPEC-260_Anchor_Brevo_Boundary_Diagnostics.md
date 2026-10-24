# SPEC-260 — Anchor Brevo Boundary Diagnostics

Status: In Progress. Authorized local implementation/tests only. Created 2026-10-19.

## Objective
Classify newly observed Anchor Brevo authorization errors before the actual HTTP provider boundary without changing eligibility or releasing historical uncertain attempts.

## Vision References
- docs/vision/Product_Thesis.md
- docs/adr/ADR-104_Instrumented_Provider_Boundaries.md
- SPEC-131 Transactional Mission Execution

## Problem
The governed wrapper marked Brevo crossed before its authorization SELECT. The October 16 missing ao_call_suppressed column therefore became ambiguous provider acceptance. Both mandatory schema columns are currently absent in production; this patch does not install them.

## Scope
Default-disabled ANCHOR_BREVO_BOUNDARY_DIAGNOSTICS_ENABLED instrumentation for the tenant-10 governed Brevo adapter. Other tenants, raw/unknown transports, email/SMS/call authorization queries, locks, timeouts, and eligibility remain unchanged. Add bounded failure stage and actual leaf-crossed metadata to Brevo failures.

## Out of Scope
Schema/admission changes, migration, release/reset of historical attempts, quarantine implementation, production configuration, deployment, and sending.

## Dependencies
Frozen boundary artifact: base 8cbeba9d1c95d74d6a658108ca9840a7044744ed, independent of draft PR956 with no layering of its code. Activation requires separate approval. Later recovery is the separate SPEC-261 artifact.

Combined-artifact metadata overlay: SPEC-261 explicitly layers the frozen boundary patch (SHA256 aa620f81bab57ec724d941ecb78b4ec2c8d47a4aab6bff60615f0ab8ac2a8d29) and PR956. Only in that combined workspace, the boundary decision is renamed ADR-104 and its references updated to avoid an ADR-102 collision with the baseline and draft PR955. The frozen boundary archive, original ADR-102 document and patch hash are unchanged. This reference-only overlay grants no runtime or production authority.

## Architecture
Only the tenant-bound opted-in wrapper carries the explicit instrumented capability. The generic shared Brevo function advertises instrumentation version but is not itself exempt from conservative outer marking. It marks immediately before axios. Authorization errors remain before-provider; HTTP and post-HTTP COMMIT failures remain crossed. Unknown/custom transports remain conservative.

## Data Model
No schema changes. No historical proof events. Failure metadata is observational and never permission to reconcile a stored attempt.

## Implementation Plan
Add failing boundary regressions, scoped instrumentation, real PostgreSQL schema-error/commit-error tests, compatibility checks, and independent review.

## Migration Strategy
None. Default off preserves legacy classification. Rollback removes instrumentation only, not records or holds.

## Testing
Offline stub-provider regressions and disposable PostgreSQL exact missing-column/COMMIT cases. Retain existing governed and spreadsheet guard suites. No live providers or production data.

## Acceptance Criteria
- Default disabled and non-tenant-10 classification unchanged.
- Original 42703 case makes zero HTTP calls; instrumented tracker remains false.
- Post-HTTP COMMIT/timeout errors remain crossed and cannot be called unsent.
- Unknown/custom transports remain conservative.
- Shared guard bytes unchanged; schema/SMS/manual eligibility not broadened.
- Historical item remains unchanged; no fabricated exact-attempt proof.

## Future Work
SPEC-261 proposes separately gated recovery and quarantine; admission policy and any migration require explicit approval.

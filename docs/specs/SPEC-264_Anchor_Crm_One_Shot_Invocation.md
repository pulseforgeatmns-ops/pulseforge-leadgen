# SPEC-264: Fixed CRM preparation one-shot invocation

Status: In progress, local preparation only. Not approved for publication, deployment or production execution. Separate sibling on frozen SPEC263 tree6f52dd9d.

## Objective
Give the existing fixed CRM preparation API a minimal auditable one-shot operator invocation, default read-only, without new routes or activation authority.
## Vision References
Product Constitution and Product Thesis: explicit operator authority, tenant isolation, observable bounded actions.
## Problem
Railway supports one-shot SSH commands, but no verified connector executes arbitrary commands. The frozen bridge is server-side only and always attempts writes. A direct unguarded API invocation has no safe preview.
## Scope / Out of Scope
One fixed command, explicit tenant10, deployed revision, full installed manifest hash and source payload hash. Read-only snapshot by default; --apply invokes only the unchanged bridge. No bootstrap, DDL, provider/discovery/network calls beyond the application's database connection, credential extraction, gate changes, arbitrary callbacks, generic admin framework or public endpoint.
## Dependencies
Exact reviewed deployed bridge; current existing activation/grant/quarantine/program/source and eligible 1–5 identity batch. Existing container database credentials stay in the container. Official Railway SSH access must be separately authorized and available; no install/login/key registration is performed by this proposal.
## Architecture
Validate flags and deployment revision before opening DB. A bounded READ ONLY transaction calls existing recovery/installed-grant validation and checks exact immutable manifest/source/current policy. Release before apply, then invoke unchanged bridge with existing activation ID pinned while retaining live kill/removal fields. Revalidation plus immutable same-ID grant rows reject replacement. CLI suppresses raw internal output and emits only fixed status/count/hash/opaque child identifiers. Dry-run is manifest/source preflight, not proof of complete contact/telemetry eligibility.
## Data Model
No schema or data model change. Only explicit --apply can create the previously reviewed bridge effects. No grants/review flags/environment variables are changed by the command.
## Implementation Plan
Fixed parser, bounded private DB pool and read-only preflight, lazy bridge invocation, sanitized output; dry-run/invalid/race/output tests and native read-only proof.
## Migration Strategy
None. Same bridge gates, counters, rollback and partial-failure limitations apply.
## Testing
Synthetic unit and native PostgreSQL checks; zero provider calls. No production execution. Verify missing/invalid flags are closed before DB, dry-run issues no writes/locks or bridge load, apply is exact and only explicit, environment replacement/removal and errors stay closed/sanitized.
## Acceptance Criteria
Exact one-shot invocation is reviewable and defaults closed/read-only; frozen parent sources remain unchanged and a separate patch/review identifies the new command.
## Future Work
No additional operator operations or automation. Publication/access/production invocation need separate approval.

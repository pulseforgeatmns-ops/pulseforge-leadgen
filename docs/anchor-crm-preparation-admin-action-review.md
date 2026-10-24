# Admin-only CRM draft preparation: local review packet

Status: implemented and tested only in an isolated local checkout. Not deployed, activated, granted or invoked against production. No email, paid discovery, enrichment or verification call is part of this action.

## Contract

POST /api/v1/tenant-outreach/anchor-program/prepare-drafts

Requires a real authenticated admin session, no impersonation or pending password reset, strict selected tenant 10, the same active tenant-10 admin in the database and installed manifest, JSON, and an Origin matching configured APP_URL. Legacy password-only admin sessions, managers, clients and malformed tenant selectors are rejected. APP_URL must be configured; no Host-header fallback is used.

The body has exactly three fields: revision (reviewed deployed 40-hex commit), manifestHash (64-hex hash of the already installed grant), and sourceMissionHash (64-hex hash of the reviewed discover source). There is no input for recipients, copy, activation, a new budget, sending, retries, provider settings or a user-controlled operation ID. Admissions and program come only from the installed grant. Canonical preparation still limits the batch to one through five exact, approved identities.

The action pins the existing activation ID, revision, manifest hash and caller. It reuses prepareExistingCrm, with action authority checked again at each locked binding. No startup DDL, SQL bypass, generic execution command, queue lease, scheduler, grant installation, review-flag mutation, freeze, send approval, dispatch, or app pool shutdown is added.

## Outcome and recovery semantics

Success is ready_for_review, with mission ID, deterministic operation ID, prepared artifact revision and candidate count. Provider readiness remains not_checked; executionApproved and envelopeAuthorized are false; sent and providerCalls are zero. This is a review artifact, not proof of provider readiness, eligibility to send or permission to contact anyone.

Failures return only a fixed sanitized error and state:

- rejected: preflight failed before calling canonical preparation; writesMayHaveOccurred is false.
- already_consumed: the canonical deterministic operation was consumed previously; this is HTTP 409, never a replayed success.
- blocked_review_required: canonical invocation failed; writesMayHaveOccurred is true because a claim or partial child may already exist.

All responses prohibit automatic retry. HTTP success loss does not authorize regenerating a batch. Existing consumption is immutable and charged before staged artifacts are persisted. Inspect the installed grant, canonical consumption and linked child read-only after any uncertainty. Never delete the claim, refund budgets, change the operation identity, rotate the grant or extend dates to force replay. Started lease recovery in the separate governed scheduler continues to quarantine uncertainty; this action neither uses nor resets scheduler leases. Recovery inspection, any proposed repair, and further preparation require separate review and authorization.

## Schema and deployment review, not execution instructions

1. Verify the exact current production schema and deployed source. Reported SPEC-261/262 tables and triggers do not establish SPEC-263 installation.
2. SPEC-262 CHECK constraints do not admit prepare_existing_crm or its item accounting. The existing docs/sql/anchor-crm-preparation-schema-proposal.sql is the separately approved, manual two-CHECK extension. It adds no tables, grants, rows or defaults. It requires explicit source/gate/approval attestations, maintenance armed, no active manifest, bounded locks, and rejects schema drift. Do not run it through application startup.
3. This HTTP action requires no additional schema beyond that proposal. Preserve all immutable consumption/quarantine rows and existing triggers. Review schema and application publication separately; neither is authorized by local implementation approval.
4. Revalidate current source, exact one-to-five identities, literal-false review flags, suppression, ownership, provenance, canonical sender/inbox and fresh database inbox telemetry. Do not release true holds or initialize NULL reviews without separate exact approval.
5. Production's historical uncertain attempt remains quarantined. Maintenance and research pause remain armed; email and research scopes must stay disabled. No automatic activation or provider readiness check is added.
6. The prior program expired on October 23 at 21:00 UTC. This code does not extend or reset it. An expired program/grant fails closed. Any replacement policy, bounded preparation grant, approval reference and activation requires a new explicit decision before a production invocation can be considered.
7. Only after separate publication/schema/authorization review, verify the exact deployed revision, canonical APP_URL, session actor and immutable grant/source hashes before reviewing an invocation. No production invocation is included in this work.

## Remaining limits

No dashboard button or user-facing selection interface is added. This is a supported explicit HTTP boundary only. The full repository test suite and production integration were not run. Local synthetic fixtures demonstrate guards and native canonical draft generation, not current production inventory or live deployment readiness. Error responses intentionally omit contact/copy/credentials and do not expose raw driver diagnostics. Read-only recovery inspection is an operator procedure, not a new recovery mutation endpoint.

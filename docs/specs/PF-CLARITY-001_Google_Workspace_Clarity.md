# PF-CLARITY-001 — Google Workspace Clarity recommendations

Status: approved implementation in progress; issue [#976](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/issues/976).

## Objective and scope
Read explicitly authorized Clarity weekly digests for Anchor tenant 10 into tenant-scoped PostgreSQL history. Max produces at most three evidence-backed hypotheses, confidence and proposed validation; the daily brief links to visual approve/reject review. No autonomous website changes, publication or deployment.

## Architecture and authority
Reuse the existing Express authenticated tenant context, PostgreSQL patterns, Command Deck living brief and SPEC-171 Canonical Execution Request/router. A narrowly bounded review-only intent records an immutable, digest-bound decision; it does not dispatch a website specialist or create deployment authority. Existing mission intents retain their boundaries.

A separate Gmail REST readonly grant is required for jacob@goanchorcleaning.com. Require the verified Clarity project ID and Gmail receiver-authenticated Microsoft sender evidence; unknown templates fail closed. Existing IMAP/full-mail tokens and ChatGPT connector access are not consent. Refresh tokens are encrypted with tenant/mailbox/project-bound authenticated encryption; bodies and secrets are never logged or persisted. Ingestion is default-off, bounded, locked per tenant, idempotent by message and reporting interval, with scheduled retry/backfill.

## Data model and migration
Add explicit integration, digest snapshot, recommendation and decision/run audit tables. All keys and lookups include client_id. Preserve nullable metrics and source provenance. Apply the migration only after separate deployment/schema approval; no startup DDL.

## Evidence and testing
Keep observed metrics distinct from hypotheses. Cross-reference only persisted, interval-matched paid-traffic/conversion facts; absence is unknown, never zero. Synthetic fixture validates parsing, duplicate delivery, malformed/no digest, retry, tenant isolation, concurrency, authorization, canonical routing and browser review. A real authorized digest remains an external acceptance dependency.

## Rollout / rollback
Ship dark. User grants dedicated readonly OAuth, operator separately approves schema/deployment and enabling the tenant. Disable the flag and integration to stop ingestion/review writes; retain history and immutable audit. Never roll back by deleting historical evidence.

## Acceptance
- Automated local/CI tests pass on the proposed commit.
- Max daily brief and visual review show bounded recommendations and exact review state.
- One real authorized digest ingested once, history and approval inspected after separately approved rollout (not claimed by this PR).

References: SPEC-171, ADR-090, PROJECT_CONTEXT safety and tenant-isolation principles.

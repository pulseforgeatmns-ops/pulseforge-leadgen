# ADR-108 — Clarity evidence and review-only resource routing

Status: proposed with PF-CLARITY-001 / issue #976.

## Context
Existing Gmail integrations use global credentials or tenant IMAP full-mail scopes. Clarity needs read-only REST access for one expressly authorized mailbox, not email sending capability. Existing SPEC-171 routes mission execution; treating a website hypothesis as an approved acquisition mission would manufacture authority.

## Decision
Store dedicated AES-256-GCM encrypted, tenant/mailbox/project/client/scope-bound OAuth credentials. The exact operator-verified project is immutable on grant reenrollment; require recognized Clarity project identity and receiver-authenticated sender evidence. Store parsed aggregate metrics and provenance, not raw email bodies. Persist integration schedules, immutable report history and recommendation decisions in explicit PostgreSQL tables with composite tenant foreign keys. The feature ships default-off with no schema creation at startup.

Add one fixed `REVIEW_CLARITY_RECOMMENDATION` CER intent owned by `clarity_review`. The existing router verifies general execution permissions then routes only this resource intent to its fixed review handler, requiring a manual identified operator, explicit tenant, authorized integration and digest-bound idempotency key. The database rechecks operator role/activity and serializes decisions. No mission is synthesized. Existing mission ownership and TME dispatch are unchanged; handlers supplied by callers cannot replace this review handler.

Review approval is an auditable state, not execution authority. It never invokes a website editor, specialist, publisher, campaign mutation or deployment. Implementation and deployment require a new explicit authorization workflow outside this feature.

Paid-platform observations and first-party attributed walkthrough leads are separately labelled, fetched only via existing tenant-scoped read-only adapters for an identical reporting window, persisted as aggregate evidence and never summed into an invented funnel. Missing facts stay null. Rules generate at most three deterministic hypotheses, with confidence in the hypothesis distinct from evidence of causation.

## Consequences
A future implementation workflow must bind a new exact change proposal and its own approval, rather than consume the review decision as permission. A grant can be revoked or polling stopped without deleting evidence. One real authorized digest and deployed UI remain separately reviewed acceptance steps; synthetic tests cannot establish them.

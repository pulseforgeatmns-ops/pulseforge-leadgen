# ADR-103: Separate non-expiring no-resend quarantine from provider outcome

Status: Proposed; local implementation/tests under SPEC-261 only. Production configuration and approval references remain unset.

## Context
An operational decision to isolate an UNKNOWN attempt is not delivery evidence. Existing review schema is incomplete, historical prepared items may be expired, and a fresh send requires exact current recipient/copy/sender and canonical approval bindings.

## Decision
Store quarantine as a dedicated immutable, non-expiring record. Retain the original item, canonical attempt, status, timestamp, reason and consumed budget. No receipt, retry, reconciliation or expiry automatically releases it. The hold is not an irrevocable business promise: a future override would need a separate explicit operator decision and implementation; no override is included here.

Keep four operational phases distinct:

1. READONLY_PREVIEW inspects existing current work with SELECT-only queries and produces a non-activatable proposal. The current preview API uses the configuration label PREPARE_ONLY.
2. APPROVED_PREPARE_EXISTING, separately authorized for one exact current-day ready artifact set, freezes one unapproved envelope/item and an audit event. It performs no discovery, enrichment, LLM generation, ordinary preparation/refill, mission/artifact edits or provider handoff.
3. Separately gated canonical approval binding verifies the real execution-approval contribution and binds it to the exact frozen envelope. It does not activate or send.
4. A separately approved exact cap-one manifest plus matching activation configuration allows only its bound tenant-10 governed Brevo item. One explicit user approval may cover binding and that exact send if both are in its scope; no duplicate approval is needed without a material change.

Missing current artifacts remain an explicit bounded-research/preparation dependency. Neither provenance nor a successful preview clears an admission hold or supplies send approval. The direct mandatory review-column read must succeed and return boolean false.

Maintain durable closure on the shared CRM email/SMS authorization paths across tenants and the explicitly guarded non-CRM mailbox-verification path. Manual/contextless and other-tenant traffic on those paths receives no recovery authority. Calls retain existing policy. This is not a guarantee that every direct Brevo or transactional/notification transport is closed.

All attempted/uncertain blockers, including missing timestamps, require individual exact quarantine coverage. Preserve raw counts. Bind canonical candidate separately from CRM contact/company; block duplicate recipient/contact/company/item identities and conservative company-name/domain aliases. Serialize apply, preparation, approval, claim and leaf handoff using the shared lock protocol. Consume the single reservation and durable handoff marker without refund, including uncertain outcomes.

## Schema and rollback
The recovery schema is a separately approved manual proposal outside auto-loaded migration paths. No review-column ALTER, default/backfill policy or row release is chosen. Preflight reports missing schema and shared-table counts by tenant.

Initial deployment is not globally zero-DDL: unrelated existing server startup schema initializers still run. This change adds no automatic recovery-schema initializer. Review existing startup effects, drain older workers and verify the covered close gate before separately approving recovery DDL.

Retain durable maintenance, quarantine/reservation records and close-capable code on rollback. Clearing the activation manifest removes authority; reverting to ungated code after schema work is unsafe.

## Identifier review
The combined artifact resolves its boundary-decision collision by using [ADR-104](ADR-104_Instrumented_Provider_Boundaries.md). ADR-104 was absent across both local decision directories, default-branch code search and draft PR955's complete changed-file list at head be45bfd432fed2f719b2618d409a5872193330ac. PR955 adds Scout ADR-102; the base also contains architecture ADR-102. PR955 adds no SPEC-260/261 or ADR-103. The local overlay updates only the boundary decision's identifier/path and linked metadata; its separately frozen archive, original ADR-102 and patch hash remain unchanged. See the operator proposal for the verified source and dependency hash.

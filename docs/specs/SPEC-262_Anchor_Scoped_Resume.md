# SPEC-262: Explicit scoped Anchor resume

Status: In progress. Authorized local implementation/tests only. Actual approvals/configuration remain unset. No publication, deployment, production writes, research-provider work or sends authorized.

## Objective
Replace the unpublished blanket-maintenance rollout with tenant-10-isolated recovery and explicit bounded program capabilities, preserving the historical no-resend email quarantine across all actual CRM email routes. Support an approved identity batch across daily envelopes/refills rather than only one frozen canary.

## Vision References
Product Constitution and Product Thesis: human authority, tenant isolation, observable conservative operations.

## Problem
The superseded unpublished SPEC261 variant closes shared CRM email/SMS and normal preparation and has no reviewed resume operation. Its permanent latch cannot be described as a temporary outage. Current production metadata lacks both ao_call_suppressed and mandatory ao_outreach_review_required. Source-only activation cannot repair that prerequisite.

## Scope / Out of Scope
Separate local overlay on frozen SPEC261 tree d1d488f621b6bc6e3037c102fa1f561cf4afe3ac56835466256c393860cfea82, full patch751662b5ae90e2ba4ecbf276f5be07efe051c210ee5831f24748d4acb11e43fb. No edits to the frozen sibling. Explicit program/policy/actor/time/cap-scoped research, preparation and email permissions; exact reviewed CRM identity batch; optional separately approved NULL-review initialization. No automatic future-discovery admission, source-derived approval, true-hold release, broad spend framework, calls change, SMS enablement or blanket all-tenant resume.

## Dependencies
Existing SPEC260/261 boundary/quarantine/claim/CAS mechanisms, current approved program and immutable source scope, canonical preparation/approval, owner/contact/provenance/verification/sender checks. Unknown policy/schema/approval closes the affected operation. ADR105 records this extension.

## Architecture
Existing control.manifest is a strict discriminated union: old exact-canary v1 remains unchanged; kind=scoped_resume v1 selects a new state only with matching explicit activation ID. The durable maintenance latch remains the tenant-10 email/preparation fallback; it is never cleared. Every non-Anchor and non-email legacy path branches before any recovery query or lock, preserving its original behavior without requiring recovery tables. The old SPEC261 parser rejects the new manifest, but its broad binary must not be used for rollback because it would pause other tenants. Revoke within the new narrow build instead. Only the exact approved tenant10/program/policy has resumed capabilities; absent, expired, malformed or removed grant closes it. Other tenant-10 programs, tenant-10 manual CRM email and tenant-10 mailbox verification remain closed. Other tenants keep their existing preparation/research/Max/email behavior. Calls and SMS retain their original queries and permissions; no new SMS permission is granted. Each permission is explicitly disabled or independently approved; email never implies paid research/preparation.

The new grant binds operator/role, current program/source/policy/scope hash, quarantine decision, finite window, canonical managed Brevo sender/inbox and reviewed identity batch. Each tuple binds prospect/company/email plus current authoritative company and provenance fingerprint. Literal-false outreach-review remains required. Identity admission is independent of envelopes/days; every item still revalidates current frozen revision/copy/canonical approval and actual HTTP payload. Normal generated copy/canonical approval is permitted only by an explicit delegated-existing-program field, not implied by identity review.

Caps intersect existing program, governor, spacing and preparation limits. Email caps count all original program attempts across grant replacements and days; an immutable per-item reservation and durable handoff CAS remain globally once-only. Uncertainty consumes budget and stops fresh work until separately reviewed quarantine. Preparation/research have explicit operation/item bounds and existing per-run limits; reservations are durable before their effects and never refunded. This release rejects research.enabled=true; paid preparation, enrichment and LLM calls remain unsupported.

Tenant-10 CRM-email denial checks use actual recipients, tenant-scoped live CRM identity, review/DNC/synthetic state and immutable recipient/contact/company/alias quarantine before any scoped send authority. Manual/SMTP calls do not gain authority; they still receive actual-payload binding and quarantine checks. Calls and SMS retain their existing policy; the missing call-only column still leaves the current SMS path fail-closed, and no new SMS permission is introduced. Admission and quarantine checks apply before selection, atomic claim and actual leaf, including new days/programs/refills. New/relinked provenance cannot turn an imported hold into approval.

## Data Model
Reuse recovery control JSONB and existing globally unique item reservations. Add a minimal immutable operation-consumption ledger only for explicit resumed research/preparation bounds where existing counters cannot safely survive failure/replacement. Manual schema only, guarded by verified tenant-10 closure; no startup migration. A separate proposed review-schema operation adds a nullable boolean with no default/backfill when absent, preserves existing schema/explicit flags, and rejects incompatible types. Exact reviewed identity initialization is a separately approved/audited NULL→false CAS with current fingerprint; true never changes. The column/table footprint is shared and must be disclosed.

## Implementation Plan
1. Freeze this contract and required invalid-by-default configuration.
2. Implement strict scoped grant, leaf/quarantine binding and capability helpers using existing mechanisms.
3. Implement narrow control/operation/admission SQL APIs and manual guarded schema proposal.
4. Integrate existing normal governed and Max paths with separate scope checks and caps; no implicit paid work or AO assignment.
5. Native lifecycle: maintenance rejects; explicit grant permits approved preparation and multiple exact batch items across daily envelopes; concurrency/daily/total/replay/UNKNOWN/hold/alias checks; durable revoke closes while retaining all quarantine/consumption. Independent hash-bound review.

## Migration Strategy
No production action approved. First approve/deploy gate-aware code and explicit closure/drain verification, then manual schema/quarantine and any explicitly approved identity review initialization. Activate only a reviewed scoped manifest; no automatic all-tenant restoration. On expiry/revoke/rollback, remaining affected scopes close. Non-Anchor flows are not paused or granted new authority by this extension. Tenant-10 unapproved scopes remain closed. Shared nullable-column DDL still takes a shared table lock and needs separate production approval; it does not authorize a global runtime outage. Never roll back to pre-quarantine binaries after schema work.

## Testing
Baseline default compatibility, old v1 canary unchanged, strict required manifest fields, actual payload substitution, imported true/relink trap, missing/NULL schema, atomic cap counters and no refunds, cancellation race ordering, exact quarantine at all CRM email routes, calls unchanged/SMS blocked, native across-days lifecycle. No real provider calls or production credentials.

## Acceptance Criteria
One integrated native lifecycle and focused adversarial review demonstrate a genuine tenant-10 bounded resume with unchanged non-Anchor behavior, not a renamed closed state; only explicitly approved identity/program/capabilities can progress. Every unresolved nonquarantined attempt still blocks. All required live approvals/values remain absent in the shipped templates.

## Future Work
Automatic discovery admission, broader programs/tenants, true review-hold releases, calls/SMS changes and expanded provider spending require separate decisions. The scoped extension is bounded normal flow, not an unconditional autonomous send policy.

## Explicit limitations
Paid research remains closed if no supported provider-operation budget can be verified; an opaque budget hash is not spending authority. Nullable/no-default review DDL intentionally does not satisfy the historical spreadsheet rollout preflight (NOT NULL/default false), and must not be reported as that rollout ready. Exact NULL-to-false admission changes a shared contact flag used by other channels and needs explicit approval despite unchanged call/SMS source. Quarantine includes the governed candidate/company and conservative company aliases, deliberately holding other contacts at that company as well as duplicate-email CRM rows.

Current operator plan: [tenant-10 scoped recovery](../anchor-scoped-resume-operator-proposal.md). Legacy suppression writers do not universally share the recovery advisory lock: final rechecks cover committed late changes but do not claim global writer serialization.


The discover-to-READY CRM-only bridge is a separately authorized local follow-on, not implemented by this frozen SPEC262 artifact. This artifact requires pre-existing READY canonical artifacts for preparation and does not authorize a Scout fallback.

## Separate local follow-on
[SPEC263](SPEC-263_Anchor_Existing_Crm_Canonical_Preparation.md) adds an explicitly approved, no-fallback CRM-only canonical preparation API for future genuinely fresh input. It has a separate source delta, manual constraint extension, item accounting and native review. It does not change the frozen SPEC262 artifact or establish current production inventory.

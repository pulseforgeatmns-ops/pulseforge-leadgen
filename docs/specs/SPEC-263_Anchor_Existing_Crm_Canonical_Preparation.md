# SPEC-263: Bounded existing-CRM canonical preparation

Status: In progress, local preparation/tests only. Separate sibling on frozen SPEC262 tree 8fb296f4f54f3391750baf347707515c762677039c820dde1c2b12454ac19b5b. No publication, deployment, production DB writes or provider calls approved.

## Objective
Prepare a fresh reviewable canonical READY batch from at most five exact newly eligible existing tenant-10 CRM identities, starting from discover with no prepared artifacts. No network, LLM, discovery fallback, enrichment, verification, execution approval, envelope activation or send.

## Vision References
Product Constitution and Product Thesis: explicit authority, observable preparation, tenant isolation and no implicit external actions.

## Problem
Verified production source is discover/Discovering with zero matching Max/Paige/Emmett artifacts and no current-day envelope/preparation. All 13 evaluated program-scoped CRM identities have prior-attempt holds. None may be reused; fresh separately approved inventory is required. Normal Scout preparation can fall back to paid discovery and therefore does not meet a CRM-only permission.

## Scope / Out of Scope
New default-closed `prepare_existing_crm` preparation operation and fixed API. Uses existing deterministic canonical Max prioritization/acquisition approach, Anchor Paige templates and native Emmett assessment. No caller-provided provider callbacks or fabricated READY contributions. Source mission/program scope remains unchanged; child preparation mission is bound to source and exact identities. Paid research, proof of provider readiness, sending and automatic admission remain excluded.

## Dependencies
Frozen SPEC262 gate/control/quarantine/immutable operation ledger; preinitialized canonical persistence schema; explicit current program/source/policy/scope and full source payload hash; literal-false review, verification, provenance, current ownership/suppression checks and immutable quarantine; fresh bounded DB infrastructure telemetry. PR955 integration and any paid inventory acquisition require separate approvals.

## Architecture
`prepareExistingCrm(pool, input, {env, now})` accepts only fixed program/source hash and a 1–5 identity subset exactly present in an installed preparation-only grant with `prepare_existing_crm` explicitly enabled. Email scope must be disabled. It validates and permanently reserves an operation before effects, then reacquires the recovery/control/source/program/identity locks and revalidates. Operation and child mission IDs derive from immutable program/source/batch identity, not caller-controlled grant/day/random IDs, preventing retry resets.

Typed CRM discovery uses only locked approved current rows. Empty, stale, mismatched, attempted, held or unavailable rows reject; there is no external Scout fallback. Canonical progression stops at READY before APPROVE_EXECUTION. Native DB telemetry and Emmett governor must be current and valid; missing/stale state blocks. Do not call adapters.infrastructure because it includes live provider checks. Return `providerReadiness: not_checked` and no execution/send authority. SPF/DKIM/DMARC and blacklist are UNKNOWN/unchecked; native planning numbers are not proof of provider or deliverability readiness. Bridge-only artifact explanations must not turn the legacy blacklist default into a claim that a check was performed. Existing send-time live gates remain mandatory later.

## Data Model
No additional tables. Reuse immutable preparation consumption, adding only the explicitly named operation to its allowlist. Each CRM draft identity consumes one preparation item and the batch consumes one preparation operation, permanently. Later freeze/refill separately consumes its own items and operation: drafting then freezing N identities needs at least 2N approved item units and two operation units. No refund or reset on failure, revocation, new grant ID, or another day. Canonical artifacts/child mission/stage audit are the exact durable effects. No source mission rewrite, contact admission, review flag change, envelope, execution approval or outbound reservation.

## Implementation Plan
1. Fixed bounded API, current exact identity/source validation and durable once-only operation reservation.
2. Opt-in preinitialized/strict DB snapshot path with unchanged legacy defaults.
3. Existing canonical deterministic executors, fixed no-fallback CRM discovery, stop READY.
4. Native discover/zero-artifact→READY regression, provider/network/DDL tripwires, stale/held/replay/cancellation negative cases; independent review.

## Migration Strategy
No autonomous schema/bootstrap. The two operation/accounting CHECK constraints are extended by the separate manual `docs/sql/anchor-crm-preparation-schema-proposal.sql`, only after SPEC262 and explicit tenant-10 closure/source/schema attestations. No auto migration; unknown constraint shape fails closed. Existing operations/grants remain unchanged. Revoke preparation authority to close while preserving all canonical/audit/consumption data and the extended CHECK constraints. The frozen narrow SPEC262 binary rejects bridge-operation grants and still counts consumed draft units; any compatible continuation needs a separate grant approval. No constraint downgrade after bridge rows exist, and no rollback to the broad-gate/pre-gate build.

## Testing
Synthetic native PostgreSQL only. Prove no pre-existing READY material, real canonical artifacts/stages and immutable source, zero external calls/DDL/sends/approvals/envelopes, strict finite identity count and prior-attempt denial, no replay through new operation/grant/day IDs, rollback closure and non-Anchor legacy compatibility.

## Acceptance Criteria
A real current eligible synthetic CRM batch reaches canonical review-only READY through existing executors with no fallback or provider calls. Missing/held inputs block before writes except permanent attempted-operation reservation where appropriate. Production remains unavailable until genuinely fresh approved inventory and separate DB-write approval exist.

## Future Work
Paid inventory acquisition, broader tenant/program admission, execution approval and sending remain separate decisions. No general provider budget or autonomous-discovery framework is introduced.

Operator proposal: [CRM-only preparation](../anchor-crm-preparation-operator-proposal.md).

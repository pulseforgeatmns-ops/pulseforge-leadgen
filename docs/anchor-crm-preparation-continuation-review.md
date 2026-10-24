# Existing-CRM preparation-only continuation

Status: local implementation and synthetic verification only. No publication, deployment, production schema change, authority installation, activation, review initialization or preparation invocation is authorized by this document.

## Authority boundary

An expired outbound program is a historical binding, not renewed authority. The distinct version-2 scoped-resume manifest admits only one exact existing-CRM preparation batch. It pins the complete original program row and source mission payload, the original program accounting namespace, exact reviewed identities and prior cumulative preparation consumption. It does not change the original policy, dates, scope, program ID, source, send approval or historical quarantine.

The full-row pin includes diagnostic columns such as last_tick_at and last_error. Establish the approved disabled/drained state before capturing the row hash; any later diagnostic or authority change makes the pin stale and fails closed. Use the provided canonical hash helper with normalized timestamp values, not a guessed PostgreSQL JSON serialization hash.

The preparation window is independently bounded to at most 24 hours and starts after the original program expires. It does not inherit or change the weekday sending window. Only prepare_existing_crm is permitted. Research and email are disabled; freeze, refill, canonical execution approval and dispatch remain unavailable. A real tenant-10 admin must match the installed operator. The manifest selects one through five exact identities, not a caller-selected subset.

The separate preparation state reader requires database maintenance armed, the application maintenance switch true, governed outbound disabled and research providers paused. It requires the exact activated manifest and no competing recovery manifest. The shared email recovery reader rejects version-2 authority even if an application flag is accidentally cleared. The shared live sending-policy validator is unchanged. Old binaries reject the new manifest version rather than treating it as email authority.

## Accounting and failures

The grant pins cumulative preparation operations/items already consumed for the original program. Its total caps admit exactly one additional operation and one item per exact admission. Existing daily and cumulative bounds still apply; no counter or epoch is reset. Installation and consumption validate the pinned baseline under existing recovery/control locks. An intervening consumption makes the authority stale rather than creating extra capacity.

Canonical operation identity remains independent of grant IDs, preparation dates and caller-generated tokens. Consumption commits before canonical effects. Duplicate requests and concurrency cannot replay a consumed batch. Partial failure or a lost successful response can leave a consumed operation and partial/complete canonical child. Inspect them read-only; never automatically retry, refund, delete history, rotate a grant or change dates to regenerate the same batch. Revocation closes authority but preserves consumption, source, child and audit history.

For version 2, a changed/subset batch is not a recovery loophole: previously charged existing-CRM preparation identities and company aliases remain excluded from a later continuation authority. The original version-1 batch contract is not changed. Reusing a charged identity after partial failure would require a separately reviewed repair capability, not another fresh grant.

Success stops at review-only READY with executionApproved=false, envelopeAuthorized=false, sent=0 and providerCalls=0. No live discovery, enrichment, verifier, LLM or provider-readiness check is part of this path. Unknown provider/DNS/blacklist readiness remains explicitly unknown. Stored CRM verification is not a new verification call.

This authority does not grant or create canonical execution approval. Existing independent operator-approval APIs are not globally disabled by this patch; invoking one is a separate action, not permission derived from this manifest. An unbound approval contribution still cannot bypass the governed envelope, live program and final provider gates.

## Separate production decisions

1. Review the exact patch and current-base CI before separately approving publication, merge and deployment. Preserve the shared send guards and all historical records. Deployment alone grants nothing.
2. Re-read the production catalog and closed tenant-10 controls. The existing SPEC-263 manual SQL extends only two consumption CHECK constraints to permit prepare_existing_crm and charge its identity count. This authority uses the existing scoped-resume grant/control/consumption schema; no extra schema is intended. Actual schema drift blocks execution. The SQL requires explicit source/closed-gate/approval attestations, 5-second lock and 30-second statement timeouts. Its table lock may briefly block concurrent access. It is never run at startup. Obtain separate exact SQL approval.
3. Produce a fresh, bounded read-only identity/alias/suppression/quarantine/ownership/provenance review. The four preliminary October 23 CRM prospects are not approved or currently cleared. Mint Maintenance remains held for buyer-fit evidence. Any NULL-to-false review initialization requires separate exact identity approval; true holds cannot be released.
4. Present the complete concrete manifest for approval: current deployed revision, admin, exact identities/fingerprints/approval references, original full program/source hashes, policy/scope/quarantine hashes, sender/inbox, current cumulative baselines, one additional operation/N items, and explicit start/end times. Preserve the original expired program. Re-read all inputs immediately before installation; stale pins stop the operation.
5. Separately approve the installation route, exact activation/settings changes and durable grant/review writes. Keep application maintenance=true, governed outbound=false, research paused=true, database maintenance armed, and email/research scopes disabled. Reuse the supported bounded operator API; no direct SQL insertion into grant/consumption tables or credential extraction. The API is not itself evidence that a secure production invocation route is available.
6. Separately approve invoking the existing authenticated same-origin admin prepare-drafts action with exactly revision, manifestHash and sourceMissionHash. Require fresh native inbox telemetry under five minutes old. The response and linked canonical artifacts must be read back; no automatic retry on uncertainty. Show exact draft recipient/copy/sender for review without approval or sending.

The schema-only decision does not approve steps 3–6. Any sending, later freeze, renewed outbound policy or paid acquisition is a different decision and cannot use this authority.

## Data-preserving rollback

Before schema transaction commit, errors roll back atomically. Afterward retain expanded constraints and all immutable rows. Revoke a subsequently installed authority through the bounded operator API only under its explicit approval, preserving database maintenance armed and app maintenance/provider pause. An old narrow gate-aware binary rejects version 2, but do not roll back to broad/pre-gate code. Do not shrink the operation allowlist after preparation consumption exists or rewrite unknown historical delivery outcomes.

## Verification limits

Local regression and disposable PostgreSQL evidence are recorded in the accompanying review report. They demonstrate synthetic guards and canonical artifacts, not current production inventory, fresh provider readiness or production end-to-end operation. No production database or provider was invoked for this implementation.

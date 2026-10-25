# Authenticated preparation approval controls

These are same-origin JSON endpoints under `/api/v1/tenant-outreach/anchor-program/preparation-approval/`. Use an existing signed-in verified admin session explicitly selecting tenant 10. No bearer/session extraction, console database access, new credentials or role change is needed. Cross-origin requests, impersonation, AO users, stale sessions and wrong tenant selections are rejected. Responses are no-store and never contain a native session ID or its digest.

## Read-only preview

POST `preview` with `{ revision, intent }`. Revision is the exact deployed 40-character SHA. Intent has exactly these fields:

- `id`: a new explicit approval/manifest identifier, also the separately approved future activation setting.
- `programId`, `sourceMissionId`: the historical program and original approved source.
- `validFrom`, `expiresAt`: fixed ISO timestamps, at most 24 hours apart. They are never extended on retry.
- `approvalReference`, `preparationApprovalReference`: explicit bounded draft-only approval references.
- `dailyOperationCap`, `dailyItemCap`: explicit finite caps. Total capacity is pinned to existing cumulative usage plus exactly one operation and the listed identities.
- `admissions`: one to five exact `{ prospectId, approvalReference }` objects. These references do not themselves change or satisfy database review state.

The server derives current full-row program and source pins, company/contact fingerprints, quarantine decision, sender/inbox configuration, current cumulative ledger baseline, and current verified preparation approver. It returns a reviewable `proposal`, canonical `manifestHash`, source/program/identity-batch hashes and baseline counts. The proposal excludes session/approver binding fields; those are reconstructed server-side. No settings are fabricated to make preview succeed.

The result includes `blockers`, `installable`, `installed` and `status`. Without the separately requested exact review plan below, a NULL candidate review state yields `outreach_review_required`. Preview never approves or initializes it. Missing settings activation yields `preparation_settings_not_activated`, without changing those settings. Canonical suppression/qualification remains `not_checked`: preview is not a claim of complete execution eligibility. Canonical preparation repeats its independent safeguards later.

Derivation runs in a bounded SQL read-only transaction. Guard validation uses the existing bounded locked transaction with no data-mutating operation. An explicitly requested review plan is simulated through the same identity/quarantine/NULL-state checks used by installation; only a local result object is changed for validation. Locks may briefly contend; 5-second lock/statement limits apply. Preview never installs a grant or consumes a claim.

## Explicit installation

After reviewing the exact returned values and obtaining all separate approvals, POST `install` with exactly `{ revision, proposal, manifestHash }`. Use the unchanged proposal and hash from preview. The server derives only the current authenticated approver binding again and requires the exact same canonical hash. It does not refresh source, program, identity or accounting pins during installation. Any drift fails closed and requires a new preview and review, not an automatic replacement approval.

Ordinary installation calls the existing guarded installer without `reviewInitialization` and requires pre-existing reviewed records. Only the separately confirmed exact-review form below may initialize selected NULL review flags. Caller actor/session fields, extra fields and a different revision are rejected. Both forms require separately configured activation settings and cannot alter settings, generate drafts or send anything.

Successful initial installation returns `status: installed`, `freshApproval: true`. Repeated identical submission returns `already_installed`, `freshApproval: false`. Neither response asserts available or unconsumed preparation capacity. A later explicit preparation action has its own one-time ledger checks.

## Separately confirmed exact prospect review

To preview a specifically authorized review decision, add `review` to the ordinary preview request:

```json
{
  "action": "initialize_null_review_false",
  "approvalReference": "the-separate-exact-prospect-review-approval",
  "prospectIds": ["an-explicit-admitted-prospect-id"]
}
```

`prospectIds` must be a nonempty, duplicate-free subset of the exact intent admissions. The server never selects NULL records automatically or enlarges the batch. It derives `reviewInitialization` containing exact prospect/company/email/fingerprint pins, plus `reviewConfirmationHash` binding the deployed revision, session-bound manifest hash and normalized review audit. `reviewInitializationHash` separately binds the manifest hash and normalized audit for durable receipt comparison across code revisions. `reviewEffects` describes the expected NULL→false transition, not observed current state; a blocked preview is explicitly `blocked_not_applied`. No preview changes data.

Review every exact identity, separate reference, fingerprint and expected transition. Obtain explicit approval for the irreversible-by-this-control review transition as well as grant installation. Then POST `install` with exactly `{ revision, proposal, manifestHash, reviewInitialization, reviewConfirmationHash }`, unchanged from that preview. Both review fields are mandatory together. A hash is an exact-content confirmation checksum, **not** a signed server issuance token or evidence of human approval. It cannot replace the actual operator review and separate authorization.

The adapter delegates to the existing guarded transaction; it has no independent review UPDATE. Current admin/session/tenant, full program/source pins, historical baseline, closed gates, quarantine and exact identity are checked under bounded locks. Only explicitly selected NULL flags can become false; TRUE holds, unknown/true DNC or synthetic state, changed identity and a NULL flag already changed elsewhere all fail closed. Every other admitted identity must already be reviewed. All selected reviews, their audit, grant and control installation commit together or roll back together. No buying intent, qualification, budget, execution approval or sending is asserted by setting the review flag.

Identical acknowledged replay requires the same persisted manifest **and** review audit and returns `already_installed`, `freshApproval: false`, `initialized: 0`. A fresh install reports the number initialized by that request. Neither refunds consumed work or offers fresh capacity. Conflicting plans for the same grant cannot replace the winning audit. A later TRUE hold still blocks preparation; revocation does not clear it.

The authenticated receipt includes `reviewInitializations`, `reviewInitializationCount`, `reviewInitializationHash`, `reviewAuditScope: historical_committed_transition`, and `currentReviewState: not_checked`. Compare the durable audit hash and exact identities against the reviewed preview after any uncertain response, **before any further mutation**. A missing grant reports no recorded review, not a claim that the prospect is currently NULL. A historical audit proves only that the stored transition committed; it is not a fresh eligibility or buying-intent check. Revoking a grant retains the audit and review state; it does not restore NULL, undo a later hold, or refund capacity. Reversing a review decision would require a separately authorized capability; this control deliberately does not provide one.

## Explicit closing

POST `revoke` with exactly `{ revision, manifestId, manifestHash, approvalReference }`. The exact stored hash is checked inside the locked revocation transaction, and only v2 preparation authority is accepted. The same audited admin and account binding may close an expired grant from a fresh verified session. Different admins or tenant bindings cannot take over the grant. Response is `revoked` or `already_revoked`. No history is deleted or refunded, no manifest is rebound, and no capacity is restored.

Mutation errors report `outcome_requires_readback` and `retryAllowed: false`, conservatively acknowledging possible writes if the response/commit was interrupted. Do not infer rollback merely from a lost HTTP response.

Read the exact outcome without mutation by POSTing `preview` with exactly `{ revision, receipt: { manifestId, manifestHash } }`. This receipt mode verifies the original stored hash/audit actor and the current same-admin session/account binding. It supports a fresh session and expired/revoked grants without changing them. It returns `not_installed`, `installed`, `consumed` or `revoked`, plus recorded/installed flags, expiration, scoped consumption counts and whether the current control matches. Grant/review-audit/control/ledger fields come from one PostgreSQL statement snapshot, followed by current live identity verification. It returns no refreshed proposal or authorization. Its zero-effect fields are explicitly scoped to this readback request; historical draft outcome remains `not_checked`, even when a durable claim is recorded. Check this receipt before any further mutation; an identical acknowledged replay is idempotent, not a new approval. A blocked ordinary preview reports `installed: null` when installation status was not established, never a false claim of absence.

## Same-origin browser workflow

The following is a request pattern, not permission to execute it against production. It uses the already signed-in browser automatically, without reading cookies, local storage or session values:

```javascript
const base = '/api/v1/tenant-outreach/anchor-program/preparation-approval/';
async function approvalRequest(operation, body) {
  const response = await fetch(base + operation, {
    method: 'POST', credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  });
  const result = await response.json();
  if (!response.ok) throw result;
  return result;
}
// Omit review unless the exact prospect review decision is separately authorized.
const preview = await approvalRequest('preview', {
  revision: reviewedDeployedSha, intent: exactApprovedIntent,
  ...(exactApprovedReview ? { review: exactApprovedReview } : {})
});
// Review preview.proposal, all hashes/bounds and blockers. Stop if blocked.
// To resolve an interrupted request without mutation:
const receipt = await approvalRequest('preview', {
  revision: reviewedDeployedSha, receipt: { manifestId: preview.proposal.id, manifestHash: preview.manifestHash }
});
// Only after explicit installation approval:
const installed = await approvalRequest('install', {
  revision: preview.revision, proposal: preview.proposal, manifestHash: preview.manifestHash,
  ...(preview.reviewInitialization ? { reviewInitialization: preview.reviewInitialization,
    reviewConfirmationHash: preview.reviewConfirmationHash } : {})
});
// Only when explicitly closing the same grant:
const closed = await approvalRequest('revoke', {
  revision: reviewedDeployedSha, manifestId: preview.proposal.id,
  manifestHash: preview.manifestHash, approvalReference: exactClosingApprovalReference
});
```

There is deliberately no call to the separate draft-preparation action in this flow. Cancellation before installation makes no server mutation. Navigating away after submission does not cancel a possibly committed transaction. Re-preview cannot renew fixed timestamps or silently adopt changed pins for an already approved installation.

## Remaining production prerequisites

Publication, fresh-base CI, merge and deployment require separate release approval. This patch adds no migration. Verify the separately installed SPEC-263 schema rather than reapplying it.

The four preliminary live prospects remain UNREVIEWED in the last observed state, and buying intent remains UNKNOWN. This local implementation has not read or changed their current state. Before selecting any, refresh the evidence and separately approve the exact review decision and mutation. TRUE holds cannot be released through these endpoints. Local synthetic fixtures are not candidate approval or production readiness evidence.

Before installation, separately approve the exact candidate/batch, settings activation, fixed window/caps and manifest. Keep maintenance armed and providers paused; explicitly disable governed outbound; verify drain, quarantine, current source/program hashes, ledger baseline, sender/inbox and verified admin tenant selection. Then obtain separate approval for one explicit preparation invocation and inspect canonical READY/no-send evidence. Program expiry and all sending gates remain unchanged.

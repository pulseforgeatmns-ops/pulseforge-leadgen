# Preparation-only admin identity correction (local review)

## Scope and authorization

This is a local-only correction to v2 existing-CRM preparation authority. It does not install a grant, apply schema, change roles/settings, deploy, invoke production preparation, call a provider, or enable sending. The accompanying authenticated-controls correction supplies same-origin preview/install/revoke routes; it adds no CLI or credential-export path.

The reported topology is original program authorizer account `19` (tenant `10`, AO) and current platform admin account `3` with NULL account tenant. These are account identifiers, not inferred human identities. Regression fixtures reproduce those bindings independently of any real data.

## Identity and audit model

- Existing v2 manifest `operatorId`/`operatorRole` and grant audit columns identify the **current preparation approver**.
- `preparationAuthority.originalAuthorizerId` identifies the historical program authorizer. The complete historical program row hash and source mission hash remain required. Original actor, authorization timestamp, policy, expired window, bookkeeping and source are never rewritten.
- Required `approverAccountClientId` is NULL for platform admin or `10` for tenant-bound admin. Required `approverSessionHash` is the SHA-256 digest of the authenticated native session ID. The raw bearer ID must only be obtained inside the trusted server request and passed transiently. It must not be extracted into chat, operator-console scripts, receipts, logs or manifests.
- Install/action/read/consume require the native session context, not merely its digest. Indexed session lookup verifies persisted authentication, exact actor/admin role, explicit active tenant `10`, no impersonation, current account safety, matching account binding and an active switchable tenant. Existing identity-tenant-selection session/binding validators are reused.
- Session, account and target rows are locked for mutation paths. Expiration uses the database clock after locks, independently of policy test clocks. v2 installation/consumption INSERTs also predicate on live session expiration. Deleted, expired, role-changed, rebound or differently selected sessions fail closed.
- v2 revocation is closing-only: the same pinned active admin and account binding can use a newly authenticated native session to close an expired/logged-out grant. This never changes the old digest or manifest, creates authority, or refunds consumption. Other actors, roles and bindings cannot use this path. v1 revocation retains its tenant-bound authorization.
- Preparation HTTP requests detach the in-memory session snapshot to avoid this action resaving stale data. This is not a claim of global logout/revocation protection against unrelated application requests.

## Preserved boundaries

The v1/send validators retain the original-authorizer and tenant-bound requirements. v2 remains a typed, finite, exact reviewed existing-CRM batch under maintenance, outbound-disabled and research-paused controls. It cannot authorize provider discovery, enrichment, verification, dispatch, freeze, refill, canonical send approval or automatic retries. Historical consumption remains in its original program namespace across sessions, grants, subsets and days. A claim remains charged even when later preparation fails.

## Verification

Focused unit tests cover platform and tenant-bound admins, stale account/session changes, missing/wrong native session context, expiration after account lock, and read-only/no-provider identity verification. Existing manifest/HTTP preparation tests remain intact with only stricter valid-session fixtures. Native disposable PostgreSQL suites verify the canonical READY path, immutable historical program/source, one-time accounting, no sends/provider calls, installation/consumption rejection and closing-only revocation. Full-suite counts belong to the final local verification report, not this source design note.

## Remaining end-to-end prerequisites (not implemented or authorized here)

1. Review and separately approve the finalized source publication/deployment and verify the deployed revision. Local tests do not establish production behavior.
2. Review and release the accompanying authenticated-controls adapter with this correction. Its preview/install/revoke routes derive native session context server-side and never activate settings, install schema, prepare drafts or call providers. A separately confirmed exact prospect-review plan may now initialize only the specifically approved NULL review flags inside the existing atomic installer; ordinary installation still cannot review prospects. The combined patch is not production activation approval; see anchor-preparation-authenticated-controls.md for exact inputs and readback.
3. Verify the actual deployment schema, including the separately gated SPEC-263 preparation schema and required account/session columns, without changing it. This note does not assert that SPEC-263 has been applied. Any missing schema, setting or permission change needs separate explicit approval. No role grants are a prerequisite of this correction.
4. Verify the live drain/quarantine and no-in-flight state again, then capture fresh complete program-row/source hashes and cumulative consumption baselines after that drain. Independently qualify the proposed candidates and obtain approval for the exact reviewed identity batch, including any separately needed review-state initialization; identity correction is not candidate approval. Obtain a current verified admin session explicitly selecting tenant `10`, and review the exact new manifest, those fresh pins, finite time window and approval references. Old v2 manifests lacking the new binding fields intentionally fail closed.
5. Separately approve any required scoped setting/activation change and exact grant installation through the trusted adapter. Maintenance/outbound/provider gates must remain closed as prescribed by v2.
6. After revision/session/binding verification, separately authorize one bounded draft-preparation invocation; inspect the canonical READY result and all no-send/no-provider evidence. Session loss after the claim is not permission to retry or refund it.
7. Any later sending, provider work, execution approval or other capability requires separate review and authority. None is implied by a ready-for-review draft or this correction.

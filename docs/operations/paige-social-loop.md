# Paige governed social loop

Audit baseline: merged `origin/main` **375a9ed**, PR #701 (Max–Scout–Emmett oversight). This change is a PR only; no deployment, account configuration, real generation, or provider publish was performed while building/testing it.

## What was implemented before this PR

- SPEC-256 canonical `social_content` generation, `paige_social_content_artifacts`, tenant/client checks, draft-only authority, pending-comment mirrors, generation entry points for dashboard/cron/CLI.
- Approval and publication services, separate approval/publish states, a compare-and-set publication claim, status inspection.
- Legacy adapters for LinkedIn through Buffer, Facebook, Google Business Profile and GitHub blog publication.
- Client-specific copy/grounding/quality rules, including Anchor voice and facilities-assessment wording.
- SPEC-092 content publications, engagement snapshots, qualitative/business outcomes; SPEC-093 advisory learning/recommendations and Max campaign-content delegation.

## What was missing or unsafe

- Max declared `paige_social_content` callable, but had no registered execution adapter.
- Approval stored a state/timestamp rather than binding exact content, destination, mission/campaign, and approver. Legacy APPROVED rows had no such authority.
- Publishing resolved global environment credentials, including a hard-coded Buffer channel fallback. Legacy adapters ignored resolved credentials and used global publisher settings.
- Buffer acceptance (even queued) and legacy void returns could count as publication. No mandatory platform read-back.
- Failure after acceptance could return to a fresh-send retry; exceptions or a crash could leave an unexplained PUBLISHING state. Provider IDs were not durably retained in the canonical artifact.
- Publication was not connected automatically to the outcome-learning record.
- Canonical generation inserted artifacts within a transaction, but attached mirrors using the pool outside that transaction. LinkedIn mirror generation also appended an unapproved PulseForge first-comment URL, including for Anchor.
- Anchor had a historical production-generation block. The enabled-agent policy remains required; the canonical path can now generate when Paige is explicitly enabled.
- Outcome/learning routes defaulted missing body scope to client 1 ahead of query scope and did not enforce bound-manager client authorization.

## Completion

The human reviews a canonical artifact and a configured account. A SHA-256 fingerprint binds the exact body, media, metadata, provenance, objective, mission, campaign, and destination. The approval stores the fingerprint, actor, account and timestamp in one locked update. Approving never publishes by default.

Max may delegate `social_content` with `authority: draft` and exactly one `constraints.allowedChannels` entry. The draft retains the delegation ID as mission ID and a campaign entity from `targetContext.entities`. Max may subsequently delegate `social_content_publish` only with `authority: execute_after_approval` and exactly one `publication` target entity containing the artifact ID. Max cannot manufacture the approval: publication always checks the stored human binding.

A PostgreSQL row lock records a durable attempt before provider I/O. The receipt is saved before read-back. PUBLISHED requires matching provider post ID, destination, platform, exact text, a published status and provider timestamp. The canonical JSON journal retains attempts, approval/content hashes, mission/campaign, account, timestamps, provider ID/URL/status and errors. Logs contain no provider credentials or raw error bodies.

- `NOT_PUBLISHED`: approved content can be sent with publishing enabled.
- `FAILED`: a definite rejected create response; only explicitly retryable failures can create again.
- `PUBLISHING`: the durable send claim exists. Concurrent calls cannot send. A crash remains blocked.
- `UNKNOWN`: acceptance is uncertain. Never retry create. Find the existing provider post and reconcile its ID; if absence cannot be established, leave it blocked.
- `VERIFYING`: provider ID is known. Retry performs read-back only, including provider queueing, moderation, errors and failed reads. It does not create another post.
- `PUBLISHED`: calls are idempotent; an incomplete outcome bridge can be retried without publishing again.

After verification, the artifact ID becomes the deterministic `content_publications.id`. The pending mirror is marked posted. The existing engagement, qualitative and business-outcome routes feed SPEC-093 learning. Draft generation retrieves relevant client learnings as advisory context. Strategy and client doctrine are never automatically changed.

## Supported first release

Text posts to LinkedIn Page/Personal via Buffer, Facebook Page via Graph API, and Google Business Profile via its local-post API. Each channel/account gets its own canonical artifact and approval. Media, automatic first comments, and blog publishing are excluded from this verified social path; unsupported payloads fail closed. Existing blog artifacts require a separate verified adapter/account completion and must not be sent through the old global fallback. Facebook/LinkedIn prospect comment tools are outside this change.

No automatic publishing scheduler was added. Existing Paige scheduling remains draft-only and client-enable-policy controlled. Max approved publication and the operator publish/retry endpoint share the same safe path. Engagement capture uses the existing manual UI/API; automatic provider analytics collection is not required or enabled by this PR.

## Operator and deployment steps before a real post

1. Merge after CI/review, then deploy. Apply existing social-artifact and SPEC-092/093 migrations if absent, followed by `migrations/2026-09-24-paige-governed-social.sql`. Do not remove the new columns while attempts exist. Roll back application code only with publishing disabled; never restore the unsafe global publisher for these artifacts.
2. Keep `PAIGE_SOCIAL_PUBLISH_ENABLED=false` while configuring/reviewing. Add explicit client-owned accounts through `PAIGE_SOCIAL_ACCOUNTS`; the default is an empty list with no global fallback. Set credential values through the deployment secret manager. Account IDs and provider channel/page/location IDs must be verified by the operator against the intended client's connected account.
3. Explicitly enable `paige` in the intended client's `enabled_agents` through the existing operator configuration flow. Preserve all other enabled-agent settings. This PR does not modify production clients or credentials.
4. Open `/paige-social?client_id=10` for Anchor. Request one draft with a specific objective/channel and optional campaign reference. Review its natural voice, evidence, absence of AI tells/generic closers/body em dashes, and facilities-assessment wording. Rejected/changed copy gets a new canonical artifact and fresh approval.
5. Review the exact destination and content, then approve that fingerprint. Existing unbound approvals require this review. Client PIN portal approvals also preview and bind the canonical artifact; operator publishing is separate. Use the publish endpoint with `{ "dryRun": true }` to validate local bindings/configuration without any provider request.
6. Confirm OAuth/token permissions, connected account health and current provider API version. Enable `PAIGE_SOCIAL_PUBLISH_ENABLED=true` only when ready. An authorized operator or Max's approved-execution delegation can then publish the approved artifact. **That first real provider call was deliberately not tested in this PR.**
7. Read back until PUBLISHED. A Buffer scheduled/sending post or GBP PROCESSING post is not yet verified. For UNKNOWN/PUBLISHING after a crash, check the provider dashboard. Reconcile only an existing exact post ID with an operator reason; fresh-send unlock is intentionally unavailable. A live PUBLISHING claim cannot be reconciled for five minutes.
8. Open `/content-outcomes?client_id=10`, select the linked publication ID and record engagement/outcomes. Evaluate with `POST /api/content-learning/evaluate/:publicationId?client_id=10` to produce advisory learning. Any strategy change still requires the established human process.

Model selection env vars (no secrets):

```bash
PAIGE_WRITER_MODEL=claude-opus-5-5
PAIGE_EVALUATOR_MODEL=claude-sonnet-4-6
```

The writer model controls public-facing draft generation and regeneration. The evaluator model controls quality scoring only. Separate variables allow quality/cost tuning independently without changing Paige safeguards, approval flow, or publication behavior.

Configuration example (references only, no secrets):

```json
[
  {
    "id": "anchor-linkedin-page",
    "clientId": 10,
    "platform": "linkedin_page",
    "provider": "buffer",
    "externalAccountId": "REPLACE_WITH_VERIFIED_ANCHOR_BUFFER_CHANNEL",
    "credentialEnv": { "accessToken": "ANCHOR_BUFFER_TOKEN" }
  }
]
```

Other account forms: Facebook uses `platform: facebook_page`, `provider: facebook`, numeric `externalAccountId`, an explicitly selected `apiVersion` such as `v25.0`, and `credentialEnv.accessToken`. GBP uses `platform/provider: google_business`, `externalAccountId: accounts/ACCOUNT/locations/LOCATION`, and `credentialEnv.clientId`, `clientSecret`, `refreshToken`. LinkedIn personal uses `platform: linkedin_personal`, `provider: buffer`. Multiple accounts for a channel require explicit account selection. Duplicate enabled provider destinations are rejected.

Review API: `GET /api/paige/social`, `GET /api/paige/social/:id/preview?account_id=...`, `POST /api/paige/social/:id/decision` with `decision`, `accountId`, `expectedApprovalHash`; `POST /api/paige/social/:id/publish`; `POST /api/paige/social/:id/reconcile` with `providerPostId` and `reason`. Scope is authenticated and checked against the user's client authorization. Request-body approver names are ignored.

## Verification

`npm run test:paige` covers approval hashes, changed artifacts, concurrent sends, safe failure retries, uncertain acceptance, read-back failures/mismatches, outcome-write recovery, tenant/account scope, Max dispatch, HTTP role boundaries, provider request shapes, client doctrine and learning regressions. Set `PAIGE_SOCIAL_TEST_POSTGRES=true` and `CONTENT_OUTCOME_TEST_POSTGRES=true` with disposable PostgreSQL binaries available to run database tests. CI supplies only a loopback placeholder database URL, disables live publishing, and uses mocked providers; no production secrets are referenced.

Provider references verified during implementation: [Buffer create text posts](https://developers.buffer.com/examples/create-text-post.html), [Buffer GraphQL reference](https://developers.buffer.com/reference.html), [Buffer statuses](https://developers.buffer.com/types/PostStatus.html), [Google local-post state and resource contract](https://developers.google.com/my-business/reference/rest/v4/accounts.locations.localPosts), [Facebook Page posts](https://developers.facebook.com/docs/pages-api/posts/). Actual account authorization and live provider compatibility remain deployment checks.

A broader Max regression run found the existing source-order assertion in `packages/max/workspace/tests/specialistInterrogation.test.js:133` failing. The same failure was reproduced in a clean worktree at baseline `375a9ed`; neither that test nor `WorkspaceEngine.js` changes in this PR. The other 35 checks in that broader run passed.

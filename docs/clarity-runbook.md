# PF-CLARITY-001 operator runbook

## Current boundary
This PR is code and synthetic validation only. It does not deploy, run a migration, authorize Google OAuth, configure credentials, read a real mailbox, change Google Ads or change a website. The feature is dark by default. Live acceptance is outstanding until the owner explicitly authorizes the separate steps below.

## Architecture
- `services/clarityGmail.js`: dedicated Gmail REST readonly grant and authenticated encryption. No global Gmail or IMAP fallback.
- `services/clarityIngestion.js`: bounded weekly polling, transient retry, durable page continuation and 7–90-day backfill.
- `packages/max/clarity/store.js`: tenant-bound integration, snapshot, delivery, recommendation and immutable decision/run history. No startup schema writes.
- `services/clarityRecommendations.js`: existing Google Ads and first-party attribution read-only adapters, only matching dates; missing remains unknown. No new platform grant.
- Max's living daily brief links to `/clarity-review`; browser renders server-selected evidence without reasoning. Session CSRF, scope and roles are enforced server-side.
- SPEC-171 owns review decisions. Approve means the recommendation was reviewed, never permission to implement or deploy it.

## Deployment and consent gates (not performed)
1. Review and approve the exact PR commit, the migration `2026-10-20-clarity-digest-recommendations.sql`, and staging/deployment separately. Apply migration explicitly to the intended environment after its ordinary backup/recovery checks. Do not run it against production without approval.
2. Follow [dedicated OAuth enrollment](clarity-oauth.md). The owner must authorize `jacob@goanchorcleaning.com` with only Gmail readonly. ChatGPT Gmail access and the existing full-mail Anchor token are not authorization. Verify the exact sender and Anchor Clarity project ID against an authentic digest before enrollment; the synthetic sender is not a claimed Microsoft production sender.
3. Review dedicated secret configuration and key rotation. Enrollment always saves `enabled=false`. Never paste tokens into tickets, chats, command arguments or logs.
4. Configure the trusted public `APP_URL` (HTTPS origin behind Railway) for review-origin validation. Never use caller-provided forwarded headers as authority. Separately approve activation of tenant 10, `CLARITY_ENABLED=true` and `CLARITY_INGESTION_ENABLED=true`. No other tenant is enabled by this pilot. The review feature uses the first flag; polling requires both and the integration's active grant.
5. In the approved scheduler, configure an hourly authenticated POST to `/cron/clarity-weekly-review` using `Authorization: Bearer <CRON_SECRET>` from its secret store. Do not place secrets in query strings. The endpoint has no GET mutation or absent-secret bypass. No schedule or production secret is installed by this PR. Hourly ticks are no-ops until `next_run_at` is due: completed reviews schedule seven days later, no digest one day later, continuation one hour later, failures exponentially retry up to one day. A per-tenant advisory lock prevents overlap.
6. For an approved bounded backfill, use `createClarityIngestion(...).run({clientId:10,backfillDays:90,force:true})` from the same configured job context and wire `prepareRecommendations` as in `services/clarityJobs.js`. `force` bypasses only due time, never flags/consent/tenant checks. A persisted page cursor resumes its original window. A positively identified invalid/repeated page token restarts pagination at the beginning of that same bounded window; snapshot/delivery idempotency prevents duplicate effects. Do not replace the window to skip failed work.

## Live acceptance after authorization
- Use one authentic weekly digest. Record its Gmail message ID and interval (not body), confirm exactly one snapshot and delivery on replay, and confirm no rows under another tenant.
- Inspect metrics and deltas against that digest; missing fields stay null and zero remains zero. Unsupported templates produce a sanitized rejection/provenance record, not invented data.
- Confirm chronological history and matching-window paid/first-party evidence. Missing access, missing columns, mismatched dates or missing conversions must remain unavailable. Attribution is not causation, and platform conversions are not first-party leads.
- Open Max daily brief and visual detail as authorized and read-only roles. Test approve, reject, double-click, stale digest, reload, tenant switching and browser Back/Forward. Verify an immutable review decision and no website/campaign/deployment side effect.
- Log the exact deployed commit and real-digest verification separately. Do not mark issue acceptance complete on synthetic tests alone.

## Monitoring and failure handling
Read `clarity_ingestion_runs` for safe status/counts/rejections/error codes and `clarity_integrations.next_run_at`, last success and consecutive failures. These records contain aggregate/provenance data, not bodies or tokens. New access errors require reauthorization by the owner, not fallback to broader scopes. Unsupported sender/template must be verified before parser or allowlist changes. Unknown commit outcome is reconciled by retrying the same bound decision key, never a different intent. A historical review cannot authorize deployment.

## Rollback
Set `CLARITY_INGESTION_ENABLED=false` and disable the integration to stop reads. Set `CLARITY_ENABLED=false` to hide the feature and block review writes. Disable its scheduler tick. Revoking the dedicated Google grant requires owner authorization; retain encrypted records only under the application's retention policy. Roll back application code only after ordinary approval. Keep additive tables and audit/history; no destructive down migration or blanket deletion is provided. Encryption-key rotation must re-encrypt with the same tenant binding or re-enroll; never silently fall back to plaintext.

## Validation commands
`npm run test:clarity` runs parser, grant, scheduler, recommendation, UI safety and non-skipping disposable PostgreSQL tests. It never accepts a production database URL. `CHROMIUM_PATH=/path/to/chrome npm run test:clarity:browser` runs nine synthetic intercepted-browser scenarios. CI provisions its own PostgreSQL and uses no provider/production credentials. Existing SPEC-171, Max and living-brief regressions run in PR CI.

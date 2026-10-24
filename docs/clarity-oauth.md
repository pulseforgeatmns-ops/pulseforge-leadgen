# PF-CLARITY-001: Gmail readonly enrollment and ingestion runbook

Status: implementation and synthetic tests only. No OAuth grant, live Gmail read,
production migration, deployment, tenant activation, website edit, or external
message was performed by this change. One authentic, authorized digest remains a
post-rollout acceptance dependency.

## Approval boundaries and owner involvement

The pilot is explicitly bound to client `10` and `jacob@goanchorcleaning.com`.
Existing ChatGPT Gmail connectors, Riley tokens, `GMAIL_TOKEN`, and Anchor's
`https://mail.google.com/` IMAP grant are not this application's authorization.
Do not reuse them. The implementation uses a separate OAuth app and Gmail REST,
not SMTP or IMAP, and cannot send, alter, or delete mail.

The owner must approve creating this persistent readonly access and complete
Google's consent screen. **The Gmail readonly scope grants access to the whole
mailbox, not just Clarity messages.** The narrower exact-sender, subject, date,
page, and runtime limits are application-enforced, not a Google permission
boundary. `gmail.readonly` is a restricted scope; public/external use can require
Google verification and, depending on data use, a security assessment. Review
[Google's scope classifications](https://developers.google.com/workspace/gmail/api/auth/scopes).

Separately obtain approval for the target database migration, deployment,
configuration/secret changes, and enabling ingestion. The enrollment command
saves a disabled integration even if it replaces an existing grant. Approving a
Max recommendation records a review decision only; it never approves website
publication or a production change.

## Google Cloud and Workspace prerequisites

1. In the owner-approved Google Cloud project, enable Gmail API. Create a
   **dedicated Web application OAuth client** that has not previously received
   wider mail permissions. Do not use the legacy Anchor IMAP OAuth client.
2. Configure the consent screen's application identity, audience, support
   contacts, and this single scope:
   `https://www.googleapis.com/auth/gmail.readonly`.
   Use an Internal audience only if that is appropriate and available for the
   Workspace organization. Otherwise configure the allowed test user or complete
   the applicable external publication/verification process. Follow the current
   [official consent-screen setup](https://developers.google.com/workspace/guides/configure-oauth-consent).
3. Register the exact redirect URI
   `http://127.0.0.1:8787/oauth/callback`. A different chosen port must match the
   registered URI and `--port` argument exactly. Google's web-server flow supports
   local testing redirects, offline refresh tokens, and state verification; see
   [the official web-server OAuth guide](https://developers.google.com/identity/protocols/oauth2/web-server).
4. If Workspace policy blocks consent, the Workspace administrator must review
   this dedicated app in the organization's app access controls. Do not work
   around the block with a different account, app password, service account, or
   domain-wide delegation. This implementation does not request those capabilities.
5. Inspect an authentic Clarity digest's original `From` address and verify it
   with the mailbox owner. Supply that exact address to enrollment. No production
   sender was provided in issue #976, so **there is no guessed sender default**.
   The current pilot additionally restricts senders to simple addresses at
   `microsoft.com`; this is a conservative implementation constraint, not a claim
   that the actual digest has been verified to originate there. If the authentic
   sender differs, stop and review/update the allowlist with a real fixture.
6. Verify the Clarity project for the actual Anchor website and obtain its exact
   project ID from the owner's project URL. The recognized form is
   `https://clarity.microsoft.com/projects/view/<project-id>/...`, also shown in
   [Microsoft's official integration template](https://github.com/microsoft/clarity-gtm-template/blob/main/template.tpl).
   No real project ID is guessed or seeded. A mailbox can receive reports for
   several projects; sender and date alone are not enough. The pilot's project
   binding cannot be changed by ordinary re-enrollment.

## Secret configuration

Use an approved secret manager to provide these variables to the local enrollment
process and, after separate deployment approval, the application runtime:

- `CLARITY_GOOGLE_CLIENT_ID`: dedicated OAuth client ID.
- `CLARITY_GOOGLE_CLIENT_SECRET`: its client secret, never a legacy client secret.
- `CLARITY_TOKEN_ENCRYPTION_KEY`: a cryptographically random 32-byte key encoded
  as standard base64 (44 characters including padding).
- `CLARITY_TOKEN_KEY_ID`: optional key identifier, default `v1`.
- `APP_URL`: trusted public HTTPS application URL for hosted review-origin validation behind Railway. Never authorize an origin from untrusted forwarded headers.
- `DATABASE_URL`: the explicitly approved target database. Enrollment requires
  the Clarity migration already present; it does not create tables.

Never put tokens, codes, client secrets, keys, or email bodies in git, tickets,
chat, screenshots, terminal capture, or application logs. Keep the encryption key
outside the database with restricted runtime access and a protected recovery copy.
The DB contains only an AES-256-GCM envelope for the refresh token. Authenticated
encryption binds client ID, mailbox, exact digest sender, Clarity project ID, OAuth client, granted
scope, and consent timestamp. Transplanting or editing those fields invalidates
decryption. Access tokens stay in process memory.

## Explicit owner-run enrollment

From the reviewed repository on the same trusted computer where the owner opens
Google's consent URL, with secret-manager-provided environment variables:

```sh
node scripts/clarityOAuth.js --confirm=authorize-clarity-client-10 --sender=<verified-address> --project-id=<verified-project-id>
```

Replace both placeholders with verified values; do not use the synthetic test
sender/project. The command binds only `127.0.0.1:8787`, prints a Google authorization URL
(not credentials), and waits up to ten minutes. Open that URL locally, choose
`jacob@goanchorcleaning.com`, verify the dedicated app and readonly permission,
and decide whether to grant access. Do not bypass browser security warnings.

The callback uses one-time state plus PKCE, exchanges the code, verifies the
actual returned scope and Gmail profile mailbox, encrypts the refresh token, and
saves `enabled=false`. A missing refresh token, extra scope, or different account
fails closed. It never prints access/refresh tokens or writes token JSON files.
If consent is denied or times out, restart only when the owner is ready.

## Separately approved activation and scheduling

The operator must review and explicitly apply
`migrations/2026-10-20-clarity-digest-recommendations.sql`; no startup DDL exists.
With separately approved runtime deployment/configuration, ingestion requires all
of the following at execution time:

- `CLARITY_ENABLED=true` and `CLARITY_INGESTION_ENABLED=true`.
- Client-10 integration `enabled=true`, `consent_status='authorized'`, matching
  mailbox/OAuth client/scope/sender, and a decryptable encrypted grant.
- A due `next_run_at` and a per-client advisory lock.

The authenticated cron entrypoint is `POST /cron/clarity-weekly-review`; send
`Authorization: Bearer <CRON_SECRET>`, never expose the secret in logs. Configure an
operator-approved periodic invocation (hourly permits bounded retry/continuation).
The stored due time, not cron frequency, governs normal weekly mailbox ingestion.
No schedule is created by importing the module or by the migration.

`createClarityIngestion(...).run({clientId:10,backfillDays:21})` is the bounded
operator/service entrypoint. Backfill accepts 7–90 days; `force:true` bypasses only
the due-time check, never any consent, flag, mailbox, or tenant gate. Use a trusted,
approved operator invocation; there is no unauthenticated backfill endpoint.

- Default overlapping search: last 21 days, exact sender and Clarity subject.
- At most 25 messages per page, two pages per run, a two-minute run budget, and
  15-second provider-request timeouts. Gmail's query and pagination behavior is
  documented in [users.messages.list](https://developers.google.com/workspace/gmail/api/reference/rest/v1/users.messages/list).
- Retry only transient network/429/5xx failures, at most three attempts per read.
  A structured invalid-page-token response or repeated cursor resets only the
  token, retaining the exact original bounded time window for idempotent replay.
- Success schedules the next poll in seven days; no accepted digest retries in
  24 hours. Failure backs off from one hour to 24 hours. Partial pagination saves
  its exact query/cursor and resumes after one hour.
- Snapshots deduplicate by tenant/reporting interval and retain delivery
  provenance. Replays also retry idempotent recommendation creation so a crash
  between snapshot storage and recommendation creation is recoverable.
- Consent is re-read before each provider attempt and persistence step. Disabling
  or revoking a grant stops subsequent reads; an already in-flight request cannot
  be recalled. Persisted cursor updates and snapshot insertion check the original
  consent generation so an old run cannot overwrite a newly enrolled grant.

## Evidence contract and verification

The parser returns verified `project_id`, `window_start`, `window_end` (inclusive seven-day dates),
`metrics`, `deltas_pct`, and `provenance`. Metric keys are `sessions`,
`pages_per_session`, `scroll_depth`, `session_duration`, `dead_clicks`,
`rage_clicks`, `quick_backs`, and `js_errors`. Session duration is seconds;
percentage metrics use percentage points (8.65 means 8.65%, not 0.0865). Deltas
are signed relative weekly percentage changes. Absent/unknown values remain
`null`; zero remains zero. No implied conversion, causal explanation, or unknown
metric is manufactured from a digest.

Provenance retains project ID, Gmail message/thread ID, receipt time, normalized-content SHA256,
parser version and MIME source type. HTML/plain MIME alternatives are not counted
twice; attachments/forwarded messages are not digest evidence. Malformed dates,
metrics, duplicate labels, wrong sender/project, and non-digests are rejected with safe
source/error provenance. Bodies and subjects are neither persisted nor logged.

Production ingestion additionally requires the first `Authentication-Results`
header returned by Gmail to identify `mx.google.com` and report both aligned
Microsoft DKIM and DMARC passes. It never accepts a later passing header after a
failed first result, an ARC header, or authentication claims in the body. This is
a conservative application rule; actual digest headers must be verified during
acceptance. Google explains how to inspect sender authentication in
[Gmail's authentication guide](https://support.google.com/mail/answer/180707).
Exactly one official Clarity project ID must be present in HTML links/plain text
and match enrollment. Missing, conflicting, lookalike-domain, or different-project
links fail closed rather than contaminate another site's reporting interval.

After approved rollout, verify one authorized real digest's report window and
metrics against Gmail, inspect its snapshot/recommendations, run the same bounded
ingestion again, and verify no duplicate snapshot/recommendations. Inspect the
daily brief and approve/reject review audit while confirming no website changes
or outbound sends occur. Synthetic unit fixtures do not substitute for this
real-template acceptance. Run local tests with:

```sh
node --test test/clarityParser.test.js test/clarityGmail.test.js test/clarityIngestion.test.js
```

## Disable, revoke, recovery and retention

Disable either ingestion flag or the tenant integration to stop new Gmail reads.
Disable `CLARITY_ENABLED` to darken the review feature too. Record authorized
revocation as `consent_status='revoked'` and revoke this dedicated Google app
through the owner's Google account when requested. Do not revoke unrelated apps.
Do not delete snapshots/review history as rollback.

`clarity_oauth_reauthorization_required`, mailbox/scope mismatch, or decryption
failures need operator review, not fallback credentials. Restore a missing key
only from the approved secret manager. For key/client rotation, disable the
integration, approve the credential/configuration change, enroll anew with the
new configuration, verify, and separately approve reactivation. Enrollment does
not log or return the old plaintext token. Malformed real templates need a safely
redacted fixture and parser tests before replay, not a permissive fallback parser.

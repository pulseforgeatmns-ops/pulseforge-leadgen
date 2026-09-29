# Studio Substral launch runbook

Public promise: a request for a person to review a website and send a written
assessment. Intake does not collect evidence, run an audit, generate a score,
or send an automated assessment. Approved visual design and narrative are locked.

## Release checklist

- [x] Domain, email, optional context; durable queue row; visible confirmation.
- [x] Retry key prevents duplicate rows after a lost response; concurrent-safe index.
- [x] Client and server validation, cross-origin routing, bounded request size,
      rate limits, native HTML POST, accessible error/confirmation states.
- [x] Operator queue: client 1, Actions badge and request card. Assessment cards
      have a human “Mark reviewed” action instead of invoking Max.
- [x] Canonical, search/social metadata, brand icons, robots and sitemap.
- [x] Publish allowlist, CNAME, release checksums, smoke script and rollback plan.
- [ ] Apply database index and deploy the backend changes.
- [ ] Create/configure the dedicated static publishing repository and deploy.
- [ ] Configure DNS, HTTPS and canonical redirect; verify on the public origin.
- [ ] Configure and verify the hello mailbox forwarding and a real reply path.
- [ ] Complete final production browser submission and operator queue read-back.

## Architecture and review route

Static site: `sites/studio-substral`. Existing deployment intent is GitHub Pages,
a dedicated repository, `main` branch, root directory. Backend: the existing
Railway `pulseforge-leadgen` service in `production`, in the `charming-trust`
project. Verified API hostname: `pulseforge-leadgen-production.up.railway.app`.

POST `/api/public/website-assessment` accepts JSON and URL-encoded forms.
The server saves a pending `agent_actions` row for client 1. This is the durable
operator notification: log in to PulseForge, select Pulseforge (client 1), and
open Actions. `/api/actions` is authenticated and tenant-scoped. Read the domain,
reply address and context, perform the review, send the assessment manually,
then mark reviewed. No email notification or automatic customer email is sent.
The operator must check the queue; email alerts are not part of this release.

A received response means the row exists, not that the assessment is finished.
The browser preserves fields and reuses the same key after a timeout/network
failure. It accepts success only with `ok`, a request reference and human mode.
Native forms without a browser key deduplicate identical requests within one UTC
day. Changing any substantive field is a new request. The database index is the
cross-process durability guarantee; the rate limiter is per-process (6 requests
per email/hour and 120 per transport peer/hour, bounded to 10,000 buckets). The
verified service has one replica. Use shared rate limiting before scaling it.

## Backend deployment — first

1. Review/integrate the launch change on current `main`; preserve unrelated app
   work. Never replace production with an old site-development branch.
2. Apply `migrations/2026-09-29-substral-assessment-idempotency.sql` with the normal
   migration connection. It is additive and safe to reapply. It requires the
   existing `agent_actions.client_id` column. Existing rows without keys remain.
3. Set `STUDIO_SUBSTRAL_CLIENT_ID=1` explicitly in Railway. An invalid explicit
   value fails closed. Do not change shared database or email credentials.
4. Deploy the reviewed commit to the existing production web service. Record
   deployment ID, SHA and previous deployment ID. Keep background workers and
   cron services on their existing configurations.
5. Run `node sites/studio-substral/build/smoke-production.mjs`. Before static
   deployment, site checks will fail; API preflight/validation must now pass.
   Never use the previous broad OPTIONS response as proof of a working POST.

Read-only production audit on 2026-09-29 UTC: web service SHA `d54b6a52`,
client 1 exists as Pulseforge, assessment queue count 0, retry index absent,
POST to the API returned 404. No production request was created by that audit.

## Static release and canonical domain

Run `node sites/studio-substral/build/prepare-release.mjs /absolute/new/directory`.
Only HTML, robots, sitemap, assets, CNAME and `.nojekyll` are exported. The adjacent
SHA-256 manifest lets you verify the release. Do not publish the full application,
repository docs, tests, `.env`, build tooling or database files.

Create a dedicated Studio Substral repository under the verified owner
`pulseforgeatmns-ops`; configure Pages `main` → `/`. No such repository existed
at the audit. Upload the prepared publish set and confirm the Pages build.
Set the Pages custom domain to `studiosubstral.com` before changing DNS.
Obtain any ownership TXT challenge from GitHub Settings → Pages: its token is
account-specific and must be copied, never invented.

At the domain DNS provider, use the following current GitHub Pages values:

| Type | Host | Value |
| --- | --- | --- |
| A | @ | 185.199.108.153 |
| A | @ | 185.199.109.153 |
| A | @ | 185.199.110.153 |
| A | @ | 185.199.111.153 |
| CNAME | www | pulseforgeatmns-ops.github.io |

These values were checked against [GitHub’s domain documentation](https://docs.github.com/en/pages/configuring-a-custom-domain-for-your-github-pages-site/managing-a-custom-domain-for-your-github-pages-site).
Replace the existing apex parking/URL-redirect record (`162.255.119.179` observed)
and conflicting `www` records. Preserve mail MX/TXT records. Do not change
nameservers. No wildcard record is needed. Wait for Pages DNS verification and
the certificate, then enable Enforce HTTPS. With apex configured, verify `www`
redirects to `https://studiosubstral.com/`; do not create a masked forwarding rule.

## Mail readiness

DNS currently uses `dns1.registrar-servers.com` and `dns2.registrar-servers.com`,
with the five Namecheap forwarding MX records. In Namecheap, Domain List → Manage
→ Advanced DNS → Mail Settings should be Email Forwarding. Keep its generated
MX and SPF records. Domain → Redirect Email → Add Forwarder: alias `hello`,
destination as confirmed in the private launch handoff. Verify receipt from a
separate sender after setup. Forwarding is receive-only; verify a usable manual
reply address separately. [Namecheap’s forwarding instructions](https://www.namecheap.com/support/knowledgebase/article.aspx/308/2214/how-to-set-up-free-email-forwarding/).

## Production acceptance and rollback

Run the smoke script from a network-enabled environment. It checks public HTML,
metadata, assets, canonical redirects, CORS and an invalid API POST. For one
clearly labelled real request, use an operator-controlled `SUBSTRAL_SMOKE_EMAIL`
and add `--submit`. It returns a reference and checks a replay returns the same
reference. In the authenticated client-1 Actions queue, confirm exactly one row
with that reference, the right email/context, `requested` stage and human mode.
Mark that smoke item reviewed. Complete a browser submission on a phone-sized
viewport and verify the visible confirmation. Check receipt at hello separately.

Recheck full-page desktop/tablet/mobile, keyboard focus, reduced motion and
no-WebGL on the public release. Measure production compressed bytes and lab
performance; field INP requires real visitor data. Targets remain LCP <2s,
CLS <0.05, INP <100ms; eager JavaScript <10 KiB gzip, deferred object <170 KiB gzip.

If persistence fails, leave the site unpublished or restore the prior static
release. Roll back the web service to its recorded previous deployment if needed;
keep the additive index and all saved requests. A static rollback must not point
to an API revision that lacks intake. Re-run smoke after rollback. Never delete
customer requests or remove mail DNS as part of rollback.

# API usage visibility and bounded spending controls

Pulseforge needs to distinguish provider bills, application estimates, and
payments. An API key that can call Places or Claude is not billing access.
Historical Places attribution exists; historical Claude tokens cannot be
reconstructed from logs that never stored usage.

## What this change covers

- `/api-usage`, linked from dashboard Analytics, is restricted to admin/manager.
- `GET /api/v1/api-usage` reports today, month to date, and the requested window,
  with provider, caller, tenant, mission, operation and model breakdowns.
- Google Places uses its existing historical ledger and new shared reservations.
  Historical rows before the new meter starts are included once. Cache hits are
  excluded from paid attempts; blocked calls are not inserted as paid calls.
- Every current runtime Anthropic SDK constructor uses `meteredAnthropic`.
  Response usage records uncached input, output, cache reads and cache writes.
  Credentials, prompts, response text and provider error bodies are not stored.
- Normal SDK retries are disabled so that invisible retries cannot spend past
  a reservation. Existing agent-level retries each make a new reservation.
- Max's primary operator digest appends a deterministic spending section.
  Both Max chat routes answer API-spending questions directly from the ledger,
  without paying a model to invent an accounting answer.
- Caps reserve capacity under a provider-wide PostgreSQL transaction lock before
  network I/O. Concurrent tenants share an allowance. Uncertain failures retain
  their reservation. Cache hits continue to work when paid calls are paused.
- Max can tighten a cap or pause a provider. Max cannot raise/remove a cap or
  resume a provider. Operators can change limits through the usage page, with
  same-origin writes and stale-version protection. Policy changes are logged.

No cap is silently selected during deployment. All existing outbound authority,
approval, DNC, spacing and deliverability rules remain owned by their current
governors. A cost limit never authorizes a send. Limits are global per provider
across tenants; this change does not create one combined cross-provider budget.

## Accounting limits

The page explicitly reports **unreconciled provider billing**. Gross Places
estimates use the highest public field tier, before free usage, volume discounts,
credits and taxes. Historical records did not store requested field masks and
cannot identify the exact SKU mix. A conservative estimate is not an invoice.

Claude estimates use the official standard pricing table checked on 2026-10-14,
including the existing Paige Opus 5.5 writer and separate cache categories.
Unknown models, non-text modalities, server tools, fast mode, data residency
premiums and very long contexts remain unpriced. An unpriced request cannot
pass a dollar cap; request-count caps can still be enforced. Streams are rejected
by this first metered adapter. Other SDK endpoints and direct diagnostic HTTP
scripts are outside this runtime adapter.

Other APIs (Custom Search, Prospeo, Hunter, SerpAPI, Brevo, Twilio and Bland)
are explicitly listed as unmetered. The displayed amounts must not be described
as total company API expenditure. Calls made outside Pulseforge using the same
provider account or keys also require provider-side reconciliation and limits.

Reservations and policy resets use UTC. Provider billing may use another
timezone or post late. Incomplete history and unsettled requests are flagged.
After a limit stops a call, a later governed scheduled run must retry it; there
is no separate uncontrolled replay queue.

## Read-only production audit

Run inside the existing production Railway service or its approved environment:

```sh
node scripts/apiUsageAudit.js --days 14 --json --confirm-production
```

The script opens `BEGIN READ ONLY`, sets a statement timeout, checks for existing
tables, and performs SELECTs. It does not bootstrap the app, apply migrations,
send messages or print credentials/connection strings. It works before the new
meter is installed and labels missing sources as unavailable.

To explain a charge, compare this output with the Google Cloud billing report
for the same time range, grouped by project, service and SKU, including credits.
If available, use a Cloud Billing export for recurring reconciliation. Claude's
organization Usage/Cost Admin API requires separate billing-authorized access;
the normal application key must not be treated as that credential.

## Operator controls and Max examples

An operator opens Analytics → API usage, reviews the observed usage and selects
daily/monthly dollar and request ceilings for each covered provider. Blank means
unbounded; zero blocks paid calls. Do not configure an arbitrary ceiling before
reviewing actual workload requirements.

Example Max questions/commands:

- `Audit our API usage` — deterministic, read-only global report.
- `Pause Google Places API requests` — reduce usage immediately through a pause.
- `Cap Google Places at $5 per day` — establish/tighten a cap; rejected if it
  would raise an existing lower cap. This is an example, not a selected policy.

The operator settings endpoint requires `If-Match` containing the policy's
`updated_at` ISO timestamp, or `none` for a policy that does not exist.

## Validation and release

`npm run test:api-usage` exercises pricing, unknown states, privacy, request
denial and the existing Places cache/attribution behavior. Native PostgreSQL
tests use only `API_USAGE_TEST_DATABASE_URL` on localhost in a disposable
database named `api_usage_test`; they test concurrent reservations, historical
cutover, uncertain costs, atomic settlement, policy audit and read-only reports.
The dedicated PR workflow supplies PostgreSQL 16 and includes the existing
Paige model-selection, dry-run and doctrine regressions.

Before release, require the new workflow and affected platform checks to pass,
then verify the production page, ledger, configured ceilings and Max response.
Billing reconciliation remains incomplete until a provider report/export is
available. No deployment or provider billing change is made by preparing this PR.

Primary references:

- https://developers.google.com/maps/billing-and-pricing/pricing
- https://developers.google.com/maps/reporting-and-monitoring/reporting
- https://docs.cloud.google.com/billing/docs/how-to/reports
- https://platform.claude.com/docs/en/about-claude/pricing
- https://platform.claude.com/cookbook/observability-usage-cost-api

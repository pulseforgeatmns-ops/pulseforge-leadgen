# Approved continuous Signal rollout — activation gated

The user approved continuous operation and a $10/month incremental budget on 2026-10-07, including the fixed email relay and one labeled test. The private feed key is user-entered directly in Railway. No optional pilot/controller is authorized. The parent must confirm webhook readiness before the test send.

## Permission scope

Publish/merge the reviewed **core-only** patch and deploy the recorded approved commit. Create one private `telegramCallerFeed` service in existing `charming-trust / production`, with one replica, a 0.25-vCPU/256-MiB ceiling, a 1GB persistent /data volume and no public domain. Use ON_FAILURE restart (up to 10 retries), with no planned expiry or auto-stop. Resource increases, additional paid plans and additional Telegram sources require new approval.

Approve persistent read-only access to only the verified `frontrunz` channel using the existing account. No joins, Telegram sends, trades, signing, wallet credentials or synthetic live research. Start the prospective cohort only after genuine source/channel health. Aggregate performance stays blinded.

## Exactly one new secret

Jake sets `SIGNAL_OPERATOR_FEED_TOKEN` once in **pulseforge-leadgen / production** using a password-manager-generated random secret of at least 32 characters. The caller service references `${{pulseforge-leadgen.SIGNAL_OPERATOR_FEED_TOKEN}}`. No value enters chat, source files or logs.

Existing `TELEGRAM_API_ID`, `TELEGRAM_API_HASH` and `TELEGRAM_SESSION_STRING` remain in PulseForge and use server-side references into the caller. Existing `BREVO_API_KEY` stays in PulseForge. No Railway project/account/workspace token, deployment-stop permission or new management controller is needed. Presence-only checks found the feed-auth variable absent; the user must provision it directly in Railway.

## Exact nonsecret activation settings

Both services, after the user selects continuous operation:
- `SIGNAL_OPERATION_MODE=continuous`
- `SIGNAL_OPERATION_START_AT=<actual approved activation timestamp>`
- `SIGNAL_MONTHLY_BUDGET_USD=<user-selected positive monthly budget>`

Caller:
- `TELEGRAM_CALLER_SOURCES_JSON=<single real frontrunz entry with observed channel pin>`
- `SIGNAL_REQUIRED_CALLER_CHANNEL_ID=<observed channel ID>`
- `SIGNAL_OPERATOR_FEED_ENABLED=1`
- Existing PORT=3099, Telegram poll=15000 and persistent state-path settings.

PulseForge:
- `SIGNAL_OPERATOR_ENABLED=1`
- `SIGNAL_OPERATOR_FEED_URL=http://${{telegramCallerFeed.RAILWAY_PRIVATE_DOMAIN}}:3099/operator-events`
- `SIGNAL_CALLER_FEED_URL=http://${{telegramCallerFeed.RAILWAY_PRIVATE_DOMAIN}}:3099/feed`
- `SIGNAL_REQUIRED_CALLER_SOURCE_ID=telegram-front-runners`
- `SIGNAL_REQUIRED_CALLER_CHANNEL_ID=<observed channel ID>`
- `SIGNAL_SHADOW_MODE=1`, `SIGNAL_SHADOW_POLL_MS=60000`, `SIGNAL_CAPTURE_POLL_MS=1000`
- `SIGNAL_OPERATOR_RELAY_ENABLED=0`, `SIGNAL_OPERATOR_RELAY_CONSENT=0` until separate relay approval.

Leave all `SIGNAL_PILOT_*` controls unset/disabled. Continuous mode does not load a readiness file, require management credentials or expire after 48 hours—even if old pilot settings exist. The start timestamp filters pre-activation events; it is not an expiry. If no model is selected, generated bundles say `unselected` and remain gated.

## Costs and monitoring

Maximum continuous feed CPU+RAM+1GB volume at these limits is approximately $8.11 per 31 days before egress, credits and incremental PulseForge/database/mail costs. Typical usage may be lower; no live workload measurement exists yet. Egress is $0.05/GB. A $10/month incremental budget is an option to discuss, not a selected or guaranteed ceiling.

The monthly-budget setting records the approved operating budget; it does not claim to enforce an invoice cap. Continuous mode has no automatic billing stop or custom cost-alert sender. Railway's existing usage dashboard/available billing notifications can be reviewed without installing a project token in the app; no new cost alerts are promised as configured. Resource limits are enforced by Railway and must be read back after deployment. No workspace hard cap is requested or changed.

The private feed is expected to operate continuously within the approved monthly model. Continued cost/health reviews or billing notifications can be separately authorized; they are not a heartbeat prerequisite for capture. The 1GB volume is part of the monthly estimate, not a pilot retention add-on.

## Remaining dependencies and sequence

1. User selects operating model/monthly budget and approves publication/deployment/private service/read-only source access.
2. User provisions the one feed-auth secret directly in Railway. Agent configures only references and nonsecret settings.
3. Deploy the approved dependency/code changes; perform the approved metadata-only `frontrunz` identity/read probe. No group joining. Generate the activation bundle using sanitized probe JSON and the user-selected continuous settings.
4. Create the private caller service with empty sources, verify resource limits and storage, then configure only the real pinned source and chosen operating model. Verify connected/available health and private URL access.
5. Enable research and immediate operator ingestion. Prove the real CA/evidence/CALL/FIRST_CALLER/cohort/market/capture chain using timestamps and quality only.
6. Relay remains disabled until consent for exactly `jacob@gopulseforge.com → pulseforgeatmns@gmail.com → this ChatGPT conversation`. Gmail event automation/filtering/dedup/receipt proof is still required for ChatGPT notification. No extra auth secret is needed for the existing Brevo integration.

FOMO remains null with a copyable CA; intended app/deeplink verification does not block core engineering. Real source access, image build, live capture and ChatGPT delivery are still unproved.

## Compact approval wording for this option

“Approve publishing/merging and deploying the reviewed core Signal changes; one private Railway caller service with a 0.25-vCPU/256-MiB ceiling and 1GB volume; and ongoing read-only access to verified @frontrunz. I select continuous operation with a $___ monthly additional-spend budget, understanding it is monitored rather than a guaranteed hard cap. I will enter the one feed-auth secret directly in Railway. No pilot expiry, Railway management token, controller stop permission, workspace hard cap, trades, joins or Telegram messages. Email relay consent is separate.”

Optional pilot code is provided separately in `signal-optional-pilot.patch`; do not publish it as a prerequisite for this option.

Sources: https://railway.com/pricing ; https://docs.railway.com/pricing/cost-control .

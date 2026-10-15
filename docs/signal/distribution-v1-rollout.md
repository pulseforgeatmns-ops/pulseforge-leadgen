# Signal Distribution / Exit-Risk V1 — local candidate

## Boundaries

Manual decision support only. No orders, trades, wallet keys, transaction signing,
wallet connectivity or inferred holdings. The monitor starts from validated
empirical Front Runners CA evidence independently of ENTRY/research eligibility and
independently of whether the operator bought. Operational tables never seed or
modify cohorts, frozen research definitions, outcomes or blinded analysis.

Tonight's bounded capacity is **one active token per configured tenant** (first eligible admission), for
the earlier of 24 hours from the original caller timestamp or the explicit activation-window end. Repeated calls for that token do not
reset the baseline or TTL. Other tokens are recorded as capacity-deferred admissions,
visible in the live view; they never silently replace the monitored token.
Admission is reevaluated while the feed retains the event, but capacity does not
promise a historical baseline. Expansion above one token is blocked in startup.

Observation views: /signal-distribution, GET /api/v1/signal/distribution and its
monitor detail route. Loading/refreshing data is read-only; the optional manual-reference form is a distinct, explicitly confirmed action. Admin/manager session, selected tenant, no impersonation;
no query-selected tenant. The separate reference POST is explicitly confirmed,
JSON-only, same-origin against configured APP_URL, and tenant checked. It accepts
a positive manually supplied USD price and reference timestamp, once only, while
the monitor is active. Reference time may precede monitor creation for an already-held position, but must be a valid non-future timestamp; it does not assert that a trade happened. No default reference or assumed purchase is invented. A later
amendment workflow is deliberately not provided.

Reference multiple is sampled price / manual reference price. Observed MFE is
the maximum sample processed *after the manual reference was saved* / manual reference price. Earlier persisted samples are not recomputed, even if the supplied reference timestamp is earlier. Milestone detection is likewise forward-only from save time. Both supplied reference time and saved-at time are shown. Neither is
guaranteed execution return or historical high. The once-only milestone defaults
to 2x and is configurable to a finite multiple >=1. Price is never replaced with
market cap. Historical price drawdown uses the observed peak on the pinned pool.

## Evidence and risk interpretation

GeckoTerminal public pool endpoints are the only source. Pool identity and base
token orientation are validated and pinned durably. No quote inversion or silent
pair switch. Price, market cap, USD pool reserve, 5-minute volume and documented
5-minute buy/sell counts/volumes are recorded only when actually present.
Unavailable fields are null/UNKNOWN. Provider source timestamp is UNKNOWN; receipt
time is not represented as an exchange timestamp. Wallet distribution, developer
identity, holder clusters and insider sales remain UNKNOWN. USD reserve decline
is not proof of LP withdrawal. Buy/sell counts are a fallback pressure measure,
not dollar-weighted flow.

Version distribution-v1.0 is an explainable heuristic, not a validated trading
model. Thresholds are fractions of observed peak / initial pool reserve value:

- WATCH: >=10% observed price drawdown, >=60% sell share, >=10% reserve-value
  decline, or missing/stale required evidence. Unknown provider timestamp is
  explicitly WATCH, so current public data cannot be labeled fully fresh NORMAL.
- HIGH: >=20% drawdown plus >=65% sell share OR >=25% reserve-value decline.
- CRITICAL: >=35% drawdown plus >=80% sell share OR >=40% reserve-value decline.
- Drawdown alone cannot produce HIGH/CRITICAL. Missing data, stale samples and
  changed pools never erase a prior HIGH/CRITICAL. A complete comparable observed
  sample can lower the state; wallet/developer UNKNOWN does not become SAFE.

Snapshots older than 3 minutes are stale; future/out-of-order/replayed samples
are ignored. Source timestamp later than sample receipt is invalid/stale.
Operational polling errors are durable visible evidence gaps and preserve risk.
Transitions, observed peaks, once-only milestones and per-channel attempts survive
restart. Delivery acceptance is not handset receipt or display proof.

## Polling and cost limits

One pinned pool read is eligible no more frequently than every 60 seconds, checked
by a five-second scheduler; initial discovery adds one request. Healthy sample
latency is roughly 60–65 seconds plus transport/provider publication latency;
neither tick speed nor receipt time proves true live freshness. A first baseline
may require two requests. No catch-up burst, per-transaction monitoring, wallet
provider, paid subscription or added provider fee is introduced.

Atomic PostgreSQL reservations cover every participating adapter attempt, including
existing retries: **6 total requests / rolling minute**, of which distribution
uses at most **2 / minute and 75 / rolling hour**. Thus >=4/min slots remain for
existing requests, but this is not a guarantee existing consumers can meet their
15s/30s research capture windows. All live GeckoTerminal-consuming processes must
install this shared budget; a process not initialized with it retains legacy
behavior and defeats the deployment-wide guarantee. The quota uses database
clock and row locks, not per-process clocks. 429 backoff is durable and at least
one database-clock minute. A denied reservation yields unavailable/deferred data,
never substituted historical evidence. Temporary quota reservation rows older than two hours are pruned; monitor evidence, snapshots, transitions, reference and delivery history are retained.

Provider calls would use the existing free public endpoint (no new subscription).
Existing Brevo and Twilio charges still apply. SMS bodies are restricted to at most 300 GSM7 single-septet characters (up to two concatenated segments). SMS attempts are additionally limited by the durable activation allowance (default5, maximum20), separately from provider polling. Per-monitor/channel five-minute cooldown suppresses
repeated risk notifications; new supported CRITICAL cause can bypass it.
The atomic milestone is independent of risk cooldown. Operational test namespace
never sends, even with transport flags. Accepted/error/ambiguous outcomes are
separate per channel; ambiguous sends become UNKNOWN without automatic replay.
Do not promise a monetary ceiling: carrier/provider rates, taxes, current plan,
and exceptional transition frequency are not verified in this local task.
Sending stays disabled until destinations and possible usage charges are approved.

## Approval-gated rollout (NOT performed in this task)

1. Review/publish the local branch only with explicit authorization. Run offline,
   disposable PostgreSQL and application startup regression gates against the
   final commit. Review fresh tenant/auth configuration and provider docs.
2. Verify live supported fields and pool/source lag separately, with approval.
   Confirm all GeckoTerminal consumers/processes are budgeted; evaluate impact on
   existing prospective captures before setting the shared-budget approval flag.
3. Approve additive operational migrations:
   2026-10-14-signal-distribution.sql, then
   2026-10-14-signal-distribution-delivery.sql. No research migration is added.
   Schema must be present or the explicit migration flag must be separately
   approved. GET routes never run migrations.
4. Configure existing SIGNAL_OPERATOR_ENABLED=1, valid operation gate/channel/feed,
   SIGNAL_DISTRIBUTION_ENABLED=1, numeric SIGNAL_DISTRIBUTION_TENANT_ID,
   SIGNAL_DISTRIBUTION_SHARED_BUDGET_APPROVED=1, MAX_ACTIVE=1, trusted APP_URL,
   explicit future WINDOW_START/WINDOW_END (maximum24h), and approved SMS_MAX_MESSAGES.
   APPLY_MIGRATIONS defaults off. Invalid distribution activation logs a blocker
   while existing operator startup continues without the new monitor.
5. First deploy with SMS/EMAIL ENABLED and CONSENT flags absent. Review real
   baseline, UNKNOWN fields, tenant-scoped page, new-admission coverage and durable
   transitions before considering channel activation.
6. Only after explicit approval of each recipient/channel and costs, configure
   SIGNAL_DISTRIBUTION_SMS_ENABLED/CONSENT=1 and/or corresponding EMAIL flags.
   SMS uses existing Twilio config and OPS_ALERT_PHONE only. Email uses the
   existing Signal operational mailbox pair from signalOperator/relay.js.
   No broad outreach agents are imported.
7. Any requested transport test needs a separately reviewed named transport-test
   flow; do not weaken TEST_ONLY records or insert fake empirical observations.
   This candidate does not include a live test-send endpoint.

Rollback: disable distribution and restart the process. This stops its timers and
removes the in-process shared-budget installation, leaving operational history
intact. Do not delete evidence or reverse schema as a convenience rollback.
Existing operator flags and independent alerts remain unchanged. Review provider
budgets after rollback; quota history is retained.


## Local verification

The candidate is based on main e0d7f718. No publication, deployment, provider probe,
live SMS/email, production database write or production migration was performed.

Focused gate: npm run test:signal-distribution.
Signal regression gate: npm run test:signal-v1.
Startup smoke: npm run test:global-smoke.

Disposable PostgreSQL tests are non-skipping and require PostgreSQL binaries.
Local validation used official Debian PostgreSQL 17.11 unpacked only under /tmp,
with the existing repository harness on loopback and a test-only TCP socket
wrapper for this sandbox. No shared database URL or service was used.

The final combined run passed 201 tests, 0 failed, 0 skipped, including Signal regression, distribution unit/route/DOM lifecycle/persistence, CI path planning and startup smoke. Actual Chromium visual QA was blocked before page load by this environment's process-singleton Unix socket restriction. No visual pass or screenshots are claimed; the loopback-only browser QA script is retained for a supported environment.
This does not prove live supported fields, deployment compatibility, provider
latency or handset receipt. Those remain separate approval-gated rollout checks.

## Bounded activation window and SMS allowance (required)

In addition to the default-off enable flag, activation requires explicit
`SIGNAL_DISTRIBUTION_WINDOW_START` and `SIGNAL_DISTRIBUTION_WINDOW_END` ISO timestamps
with a timezone. No actual times are supplied by the build. Duration must be
positive and at most 24 hours. First initialization must precede the start by
PostgreSQL's clock; restarting the exact persisted window is allowed, but changing
its SMS allowance is rejected. Overlapping windows for the same tenant/namespace
are rejected. A new future window requires separate operational approval.

Only genuine caller events whose original occurredAt is at or after window start
are admitted. Buffered older calls cannot start a monitor. The first eligible CA
uses the single active slot; other admissions are visibly deferred. Each monitor
expires at the earlier of call+24h or window end. The shared research start time
is unchanged. No automatic renewal or next-window enrollment exists.

DB-clock admission, claim, observation and quota checks enforce expiry. Provider
and delivery transports additionally check a conservative monotonic deadline
immediately before invocation. No new distribution requests or notifications are
initiated once that deadline closes. Requests initiated before it may finish
later; observations arriving after closure are rejected. The DB-only view labels
expired monitors even when the browser clock is skewed. Legacy rows without an
activation are not eligible for polling or sending.

`SIGNAL_DISTRIBUTION_SMS_MAX_MESSAGES` defaults to **5**, permits integer **0..20**,
and is immutable per persisted activation. A DB transaction reserves one allowance
for each SMS send authorization across processes, before provider invocation.
Restarts, failures, UNKNOWN outcomes, crashes and unused reservations do not refund
it. Both risk and milestone SMS use this same cap. This limits message attempts,
not dollars or segments: the compact 300-character GSM7 body can use up to two
segments. Provider pricing/recipient approval is still required. Email remains
separately consented and window-bound. TEST records still never send.

Outside the configured window, participating existing Gecko consumers return to
their original pacing without recording shared quota attempts. During the window,
all consuming processes must install the same durable budget; global6/min can
defer scheduled research captures. No change to empirical definitions or
replacement of missed historical captures is authorized. Do not claim the global
cap if another live consumer has not been coordinated.

Migrations now also add activation rows/monitor linkage and per-delivery
send-authorization timestamps. Apply the reviewed additive SQL with activation
off, then approve exact tenant, start, end, recipients and message allowance.
Neither merging the draft nor applying SQL authorizes configuring these values.

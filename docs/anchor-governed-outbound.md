# Anchor governed daily outbound

This release implements an opt-in, finite standing delegation for Anchor (tenant 10). One operator authorizes a reviewed mission scope, sender, reply inbox, AO owners, expiry and budget. Max then creates a fresh artifact-bound envelope each weekday without asking Jake to approve individual messages or daily batches. Defaults are at most five first-touch attempts per New York business day, at least 60 minutes apart, up to 100 attempts over at most 30 days. A healthy verified supply and Emmett capacity are required to reach five; the target never overrides a stop condition.

The implementation is off until a program is explicitly activated **and** `ANCHOR_GOVERNED_OUTBOUND_ENABLED=true`. Keep `clients.autosend_enabled=false`. Merging, installing the migration, authorizing a shadow program, and preparing a shadow batch do not send email. Preparation does perform bounded Scout discovery, CRM admission and real enrichment/verification.

## Authority and execution

1. `review` canonicalizes a proposed policy and hashes it with the immutable approved source mission scope. `authorize` accepts that exact review hash and records the operator. New programs start in `shadow`. Only one nonrevoked program can exist for tenant 10.
2. The scheduled worker holds a tenant advisory lock and creates a deterministic daily child acquisition mission. The original approved objective, market, geography and constraints remain fixed. It runs the existing CER → TME specialist path: Scout → enrichment → Max prioritization → acquisition approach → Paige → Emmett. It does not introduce a second copy generator or a direct-send agent.
3. Enrichment attempts at most 15 candidates per preparation run. Duplicate/contact failures quarantine that candidate. Only verified, projectable CRM emails enter Max's top five. Preparation retries at most three times per day, with an hour between attempts; the next weekday starts fresh within the same authorized scope. Completed frozen batches are never silently refilled or rewritten.
4. The envelope freezes exact candidate/CRM/company IDs, recipient, subject, body, sender, prepared-artifact revision and a full manifest hash. `APPROVE_EXECUTION` creates the normal execution approval with the standing grant and envelope binding. It retains the granting operator's identity and explicitly labels the bounded delegation; it does not claim that Jake manually reviewed that day's text.
5. Each tick executes at most one envelope item through CER `EXECUTE_OUTBOUND` → TME → Emmett → existing Brevo adapter. It rechecks grant, source scope, current artifacts, exact copy, recipient mapping, email verification, DNC, prior contact, AO ownership, inbox freshness, sender readiness, capacity, hours and spacing. A short transaction commits the unique attempt and consumes the budget before the provider call. A final check catches intervening pause/suppression. A provider message ID is required for success.
6. The legacy mission executor cannot use a governed approval without the guarded dispatcher, and the generic mailbox scheduler is blocked while a standing program exists. Legacy autosend must stay disabled. Partial ticks retain `executionSummary.complete=false`, preventing provider observations from advancing the mission before the envelope is settled.

Only first touches are delegated in this phase. Generic no-response, nurture, quote, proposal and reactivation messages are not automatically authorized. Replies and human activity route to explicit lifecycle states and human tasks. Adding an automated second touch requires a separately reviewed, artifact-bound policy extension; this grant cannot widen itself.

## Canonical records and events

| Record | States / purpose |
|---|---|
| `acquisition_outbound_programs` | `shadow`, `active`, `paused`, `revoked`; immutable hashed bounds and source scope, operator, health |
| `acquisition_outbound_envelopes` | `frozen`, `authorized`, `complete`, `expired`, `cancelled`; one per tenant/local date |
| `acquisition_outbound_items` | `pending` → `attempted` → `sent` or `uncertain`; pre-send suppression, expiry and audited reconciliation to `failed` |
| `acquisition_outbound_lifecycle` | Persistent email/company suppression, semantic state and Max next action; DNC is absorbing |
| `acquisition_outbound_preparation` | Deterministic mission, daily preparation attempt budget, backoff and error |
| `acquisition_outbound_replies` | Durable raw receipt, classification, retry count, error; intake survives classifier failure |
| `acquisition_outbound_inbox_health` | Last completely successful poll per bound inbox |
| `acquisition_outbound_events` | Deduplicated grant, envelope, eligibility, send, reply, provider, AO, booking, failure and reconciliation facts |
| `acquisition_outbound_learning_facts` | Queryable tenant/mission/recipient linkage over those durable facts |

Existing mission contributions, approvals, execution records, provider observations and their learning path remain authoritative in their respective domains. The additional event ledger preserves daily-envelope provenance and semantic/AO facts for learning and analysis. Opens are telemetry, never a claimed conversion. Learning does not change sending authority or automatically expand caps.

## Reply and AO behavior

Poll the explicitly bound Anchor IMAP integration every minute. At durable receipt, match the sender to tenant 10 CRM contacts and suppress the email/company immediately, before calling Riley and even if `In-Reply-To` is missing. This is immediate at ingestion; IMAP polling adds up to the polling interval plus processing latency. Inbox health older than five minutes blocks outbound. Parse/checkpoint/UID-validity failures do not advance the checkpoint or mark a poll healthy. A successful full poll is needed to resume.

Database triggers also suppress on correlated provider stop events, inbound mailbox messages, reply touchpoints, DNC/booked CRM changes, AO lead changes and AO task changes. Pending daily items and generic scheduled follow-ups are cancelled. AO records without a CRM link fall back to exact company-name matches within tenant 10. Ambiguous/unmatched account identity remains an operator data-quality concern; do not activate until AO links are reconciled for the intended target population.

| Riley classification | State | Max action |
|---|---|---|
| interested | engaged | AO handoff |
| quote_request | quote_requested | AO handoff |
| incumbent_vendor | incumbent_vendor | AO backup/overflow conversation |
| not_now | nurture | review nurture timing |
| out_of_office | paused | review return date |
| wrong_person | wrong_contact | research correct contact |
| unsubscribe | dnc | stop and set CRM DNC |
| negative | closed | stop |
| unknown | reply_received | review reply |

Max creates a dashboard action and, for human handoffs, an AO lead/task assigned to an active approved owner. Existing human ownership remains in control. Classification retries three times; after that one attention card requests manual review while suppression remains in force. Reply polling continues after pause, expiry or revocation, including previously bound inboxes. No automated reply is sent by Riley or Max.

## Failure, idempotency and kill behavior

- Advisory locking prevents overlapping outbound workers. Database uniqueness enforces one envelope/day and one lifetime attempted email/company across grants, days and regenerated revisions. This phase deliberately does not retry a previously attempted recipient.
- Both attempts and ambiguous outcomes consume the budget. Any uncertain attempt blocks further outbound until reviewed. A crash before durable claim is safe to retry; after claim it is conservatively uncertain. An abandoned claim becomes `uncertain` after five minutes. There is no automatic timeout resend.
- Provider acceptance and PostgreSQL commit cannot be made one distributed transaction. The design chooses conservative at-most-one provider attempt and possible undersending. It does not promise exactly-once delivery. A confirmed provider ID is stored in the send ledger; canonical execution evidence is also persisted by the existing adapter.
- Stale artifacts, changed scope/sender, invalid CRM binding, missing suppression triggers, unreadable telemetry, stale inbox, DNC, human ownership, exhausted capacity, disabled legacy-safety setting or missing AO coverage stop the affected work. Meaningful blocked states produce deduplicated Max attention cards; ordinary spacing/hours/caps are quiet.
- Pausing or revoking immediately cancels all pending envelopes/items. Resuming never revives them; a fresh weekday envelope is required. The environment flag is a second stop control and must be changed on every worker process. A provider call already accepted/in flight cannot be recalled.
- Missed slots/days are not caught up, and unused daily allowance does not roll over. Authorization expiry/total cap requires explicit renewal, not daily attention. Do not delete history or clear attempt timestamps to regain capacity.

## Production rollout — commands to run after PR review

Nothing in this document has been executed against production as part of implementation. Use the production application shell with its existing secret environment, from the deployed repository root. Do not paste secrets into task output. Substitute verified IDs below; no live mission, sender, mailbox or AO identity is assumed by this release.

### 1. Deploy disabled, inspect prerequisites, then migrate

Merge the reviewed PR and deploy its commit with `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false`. Ensure the same value on every app/worker instance. Leave existing legacy autosend disabled; stop any separately configured Anchor one-send/recovery jobs before the canary.

Read-only inventory:

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 <<'SQL'
SELECT id,name,active,autosend_enabled,sender_email,sending_domain FROM clients WHERE id=10;
SELECT id,stage,status,objective,payload->'structuredMission' AS approved_scope
  FROM acquisition_missions WHERE tenant_id='10' ORDER BY updated_at DESC LIMIT 20;
SELECT id,mailbox_address,status,imap_host FROM tenant_mailbox_integrations WHERE tenant_id='10';
SELECT id,name,active FROM users WHERE client_id=10 ORDER BY id;
SELECT unnest(ARRAY['acquisition_missions','acquisition_mission_outbound_executions',
  'acquisition_mission_provider_events','tenant_outreach_messages','tenant_outreach_scheduled_sends',
  'prospects','companies','touchpoints','agent_actions','ao_leads','ao_follow_up_tasks','email_events']) AS required_table,
  to_regclass(unnest(ARRAY['acquisition_missions','acquisition_mission_outbound_executions',
  'acquisition_mission_provider_events','tenant_outreach_messages','tenant_outreach_scheduled_sends',
  'prospects','companies','touchpoints','agent_actions','ao_leads','ao_follow_up_tasks','email_events'])) AS present;
SELECT crm_prospect_id,business_name FROM ao_leads WHERE client_id=10 AND crm_prospect_id IS NULL;
SELECT event_type,event_at,sender_identity_status FROM email_events WHERE client_id=10 LIMIT 1;
SQL
```

Require `active=true`, `autosend_enabled=false`, an approved immutable source mission, a working inbox matching the canonical sender, at least one active authorized AO owner, and existing baseline tables/columns. Resolve missing baseline schema or AO identity links through the application's normal migrations before continuing. Confirm the sender's provider verification and reply inbox credentials; no test send is needed for these checks.

Take the normal database snapshot, then apply the additive migration (safe to rerun):

```sh
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f migrations/2026-09-18-anchor-daily-outbound.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -c "SELECT tgrelid::regclass,tgenabled FROM pg_trigger WHERE tgname='acquisition_outbound_observe' ORDER BY 1;"
node scripts/anchorDailyOutbound.js status
```

Require six enabled suppression triggers. If a prerequisite table was absent when migrating, stop, restore the normal prerequisite schema, and rerun this migration. No program should exist on a first installation.

### 2. Create a one-send shadow canary

Set these nonsecret identifiers from the verified inventory:

```sh
export ANCHOR_SOURCE_MISSION_ID='REPLACE_WITH_APPROVED_MISSION_ID'
export ANCHOR_SENDER='REPLACE_WITH_CANONICAL_SENDER_EMAIL'
export ANCHOR_INBOX_ID='REPLACE_WITH_ACTIVE_INTEGRATION_ID'
export ANCHOR_AO_IDS='[REPLACE_WITH_NUMERIC_AO_USER_IDS]'
export ANCHOR_OPERATOR='REPLACE_WITH_AUTHORIZING_OPERATOR_ID'
mkdir -p work/anchor-rollout
node <<'JS'
const fs=require('fs');
fs.writeFileSync('work/anchor-rollout/policy.json',JSON.stringify({
  sourceMissionId:process.env.ANCHOR_SOURCE_MISSION_ID,
  senderEmail:process.env.ANCHOR_SENDER,
  inboxIntegrationId:process.env.ANCHOR_INBOX_ID,
  aoOwnerIds:JSON.parse(process.env.ANCHOR_AO_IDS),
  startsAt:new Date().toISOString(),
  expiresAt:new Date(Date.now()+3*86400000).toISOString(),
  dailyCap:1,totalCap:1,spacingMinutes:60
},null,2));
JS
node scripts/anchorDailyOutbound.js review --file work/anchor-rollout/policy.json --operator "$ANCHOR_OPERATOR" > work/anchor-rollout/review.json
cat work/anchor-rollout/review.json
```

Inspect the exact source scope and bounds. To authorize that reviewed result, preserve its normalized policy and hash:

```sh
node <<'JS'
const fs=require('fs'),r=JSON.parse(fs.readFileSync('work/anchor-rollout/review.json','utf8'));
fs.writeFileSync('work/anchor-rollout/authorization.json',JSON.stringify({...r.policy,reviewHash:r.reviewHash},null,2));
JS
node scripts/anchorDailyOutbound.js authorize --file work/anchor-rollout/authorization.json --operator "$ANCHOR_OPERATOR" > work/anchor-rollout/grant.json
node scripts/anchorDailyOutbound.js poll
node scripts/anchorDailyOutbound.js tick --confirm bounded-anchor-execution
node scripts/anchorDailyOutbound.js status > work/anchor-rollout/shadow.json
```

Do this on a weekday after the grant starts. Require one frozen item, zero provider attempts, current successful inbox health, exact recipient/company identity, verified email, safe Paige copy, sender match, no DNC/reply/AO collision, and artifact/manifest hashes. The review is available in `shadow.json` under `envelopes[].manifest`. If inventory or capacity is blocked, repair the evidence and wait for bounded preparation backoff; do not widen or override the gates.

### 3. Activate the canary only when ready to send

This is the first rollout step that authorizes a production send. Set the production environment flag to `true` on the serving worker after reviewing the shadow artifact, leaving `autosend_enabled=false`. Extract grant identifiers and activate:

```sh
ANCHOR_PROGRAM_ID=$(node -p "JSON.parse(require('fs').readFileSync('work/anchor-rollout/grant.json','utf8')).id")
ANCHOR_POLICY_HASH=$(node -p "JSON.parse(require('fs').readFileSync('work/anchor-rollout/grant.json','utf8')).policy_hash")
node scripts/anchorDailyOutbound.js mode --id "$ANCHOR_PROGRAM_ID" --mode active --policy-hash "$ANCHOR_POLICY_HASH" --operator "$ANCHOR_OPERATOR"
node scripts/anchorDailyOutbound.js poll
node scripts/anchorDailyOutbound.js tick --confirm bounded-anchor-execution
node scripts/anchorDailyOutbound.js status
```

Run in the 09:00–17:00 New York window. Expect exactly one `sent` item with a provider message ID, matching canonical execution record, and no second attempt on repeat ticks. If `uncertain`, stop and reconcile. Inspect provider acceptance/delivery/webhook correlation and the received message before expanding. Confirm the receiving route captures a reply and creates the expected suppression and human handoff; perform synthetic reply/failure injection in staging, never by inserting fabricated production customer evidence.

### 4. Graduate to five/day and schedule unattended operation

After successful canary evidence, revoke the canary grant. On the **next New York business day**, create and review a new policy with `dailyCap:5`, `totalCap:100`, a chosen expiry no more than 30 days after its start, and the same verified scope/sender/inbox/AO owner set. Repeat the review → authorize → shadow inspection → activate commands above with new policy files and IDs. A tenant/date already used by the canary cannot receive a replacement envelope; do not work around that constraint.

Configure the existing production scheduler to make these authenticated POST requests. Schedule in any timezone; the worker applies New York/DST/weekday gates itself. Use server-side secret storage for the bearer header.

| Job | Schedule | Request |
|---|---|---|
| Anchor reply intake | Every minute, all days | `POST /cron/anchor-outbound-replies` |
| Anchor bounded outbound | Every five minutes, all days | `POST /cron/anchor-daily-outbound` |

Header: `Authorization: Bearer <CRON_SECRET>`. These endpoints reject absent/invalid secrets and provide no GET mutation. Configure a request timeout sufficient for discovery/enrichment; a client timeout does not authorize another provider attempt. Overlapping retries return `overlap`. Polling remains enabled after the finite send grant ends.

Normal daily operation requires no message or batch approvals. Max assembles/finalizes each day, sends at most one per tick at least an hour apart, and surfaces exceptions. Review and explicitly renew before the finite expiry or total-cap boundary; no silent renewal is implemented.

## Observation and exception response

`node scripts/anchorDailyOutbound.js status` and authenticated admin/manager `GET /api/v1/tenant-outreach/anchor-program` expose policy, health, the last ten envelope manifests/counts and fifty events. Only tenant 10 operators can manage this API. Authorize and mode changes are under the same API prefix at `/authorize` and `/:id/mode`.

Monitor scheduler failures, `last_tick_at` older than 15 minutes during business hours, inbox health older than five minutes, uncertain attempts, preparation shortfalls, and unclassified replies at three attempts. Max attention cards are created for actionable blocked ticks and classification exhaustion. The hosting scheduler/monitor must alert the responsible operator if the worker stops running entirely; a stopped worker cannot report its own failure.

Read-only operational checks:

```sql
SELECT id,mode,last_tick_at,last_error,policy->>'expiresAt' AS expires_at FROM acquisition_outbound_programs ORDER BY authorized_at DESC;
SELECT local_day,status,jsonb_array_length(manifest) AS planned FROM acquisition_outbound_envelopes ORDER BY local_day DESC LIMIT 10;
SELECT status,count(*) FROM acquisition_outbound_items GROUP BY status;
SELECT integration_id,last_success_at FROM acquisition_outbound_inbox_health;
SELECT id,attempts,last_error FROM acquisition_outbound_replies WHERE classified_at IS NULL;
SELECT event_type,count(*) FROM acquisition_outbound_learning_facts WHERE created_at>now()-interval '7 days' GROUP BY event_type;
```

For ambiguous acceptance, inspect provider logs using the frozen recipient, timestamp, canonical idempotency key and any message ID. Do not infer nonacceptance from a timeout. Record evidence explicitly:

```sh
node scripts/anchorDailyOutbound.js reconcile --item ITEM_ID --outcome accepted --provider-message-id CONFIRMED_PROVIDER_ID --evidence 'Provider log reference and acceptance timestamp' --operator "$ANCHOR_OPERATOR"
# Only with affirmative provider evidence of nonacceptance:
node scripts/anchorDailyOutbound.js reconcile --item ITEM_ID --outcome not_accepted --evidence 'Provider investigation reference confirming no acceptance' --operator "$ANCHOR_OPERATOR"
```

Neither command resends or restores the consumed budget. Reconciliation repairs a matching attempted/failed canonical record when acceptance is confirmed and records the operator/evidence in the ledger.

## Kill switch and rollback

Fast durable stop, using the current program ID:

```sh
node scripts/anchorDailyOutbound.js mode --id "$ANCHOR_PROGRAM_ID" --mode paused --operator "$ANCHOR_OPERATOR"
```

Also set `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false` on every worker and disable the outbound scheduler job. Keep the reply-poll job running. Verify pending items are suppressed, envelopes cancelled, no new attempt appears, and any in-flight provider result is accounted for. Pausing does not retract an already accepted message.

Operational rollback is additive: retain all new tables, triggers and audit history; leave the program paused/revoked and the flag false. Roll back the application commit only after disabling all Anchor outbound entry points, because older code does not know the new governor. Continue monitored reply intake on the new code until outstanding replies are handled. Do not drop the journal, clear suppression, delete grants, or reset attempt timestamps to resume sending.

## Validation

`npm run test:anchor-outbound` uses disposable local PostgreSQL and fake provider transport. It exercises finite policy/DST, strict eligibility/copy binding, canonical preparation and approval, partial mission completion, shadow mode, competing ticks, spacing/caps, ambiguous acceptance, reconciliation, reply/AO/provider suppression, replay, DNC/artifact/CRM drift, kill switches, late replies, failed classification, legacy-path blocking and expiry. The migration is applied twice to verify rerun safety. The added CI workflow uses no production secrets.

The mailbox/scheduler/revision regression command in `.github/workflows/anchor-governed-outbound-tests.yml` covers existing behavior. Production deliverability, real account readiness, live inventory depth and AO identity coverage remain rollout checks; local tests cannot establish them.

Local validation at implementation: 19 governed-outbound tests and 97 mailbox/scheduler/revision regression tests passed. A broader six-file execution/sender check had 32 passes and 11 failures, with the identical failures reproduced against unchanged `main` at `7b5121f`. These comprise seven SPEC-071 fixture/sendability failures, three SPEC-186 sender/fixture failures, and the SPEC-212 binding test's missing `ava` dependency. This release does not claim that the repository-wide test suite is green.

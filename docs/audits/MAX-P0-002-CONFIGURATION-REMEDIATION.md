# MAX-P0-002 — Production configuration remediation package

Assessed against the **deployed** tree, Render live commit `69e7216` (Merge PR #1161), not branch head.
All file/line references below are at `69e7216`. Nothing in this package has been applied.

`69e7216` predates the MAX-P0-003 code (`services/missionLease.js` and `services/maxReliability/` are not in it). This means
the unapplied P0-003 tables are **expected** and do not affect the running service. Do not apply them as part of this package.

---

## 0. Summary of required changes

| # | Variable | Current | Required | Class | Restart needed |
|---|---|---|---|---|---|
| 1 | `SESSION_SECRET` | unset → hard-coded public fallback | **set**: random, ≥64 hex chars | Security, required now | yes |
| 2 | `ANCHOR_RESEARCH_PROVIDERS_PAUSED` | unset → pause **not armed** | **`true`** | Containment, required now | yes |
| 3 | `PF_MIGRATION_STAGING` | `0` | delete (no behaviour change) | Hygiene, optional | same restart |
| 4 | `ANTHROPIC_API_KEY` | unset | restore **only after** the spend-policy check in §2 | Function, owner decision | yes |
| 5 | `GOVERNED_OUTBOUND_TENANT_17_ENABLED` | unset | **no change** (unset = off) | none | — |
| 6 | 21 historical records | unresolved | read-only classification now; no `--apply` | Data, §5 | — |

Do changes 1–3 (and 4 if the owner approves it) in **one** Render Environment save, followed by **one** same-commit redeploy (§6).

---

## 1. `SESSION_SECRET` — insecure fallback is active

**Effective mechanism.** Two places fall back to a literal that is in the repository:

- `server.js:139`: `secret: process.env.SESSION_SECRET || 'pulseforge-secret-key'`. This key signs the `connect.sid` cookie (express-session + connect-pg-simple).
- `utils/sessionLifecycle.js:32`: `credentialTag()` HMACs `session:<userId>:<passwordHash>` with the same fallback. The tag binds each session to the password it was opened with.

Neither place warns or fails closed. No other secret in the deployed app has a literal fallback (a grep for `*SECRET|KEY|TOKEN || '<literal>'` finds only empty-string defaults).

**Risk.**
- The cookie signature is no longer a control. Anyone who knows the repo literal can sign any session ID.
- Session IDs are still 24 random bytes, so you can't guess one. But anyone who gets **any** row of the `session` table can now produce a working cookie for it, including admin sessions. Ways to get a row include a database read, a dump, a migration artefact, or the pgAdmin exposure (C11).
- The credential tag can also be computed by anyone with read access to `users.password_hash`.
- Severity: **high**. It is not remotely exploitable by itself, but it turns any session-table disclosure into an account takeover.

**Change.** Generate a value (for example `openssl rand -hex 64`), set it on the Render production web service only, and don't reuse it anywhere else.

**Side effects (expected).**
- Every existing session ends: old cookies fail signature verification, and the credential tags change.
- All users, including William and the closers, must sign in again. Any in-flight fresh-confirmation (step-up) marker is lost.
- Old `session` rows become unusable and expire naturally. Purging them is optional and is not part of this package.

**Verify.**
1. Render Shell: `node -e "const s=process.env.SESSION_SECRET||'';console.log(s.length>=64 && s!=='pulseforge-secret-key')"` → `true`.
2. A browser carrying a pre-change cookie is redirected to `/login`.
3. A fresh login reaches `/dashboard`. Ticking "Remember me" still persists across a browser restart.

---

## 2. `ANTHROPIC_API_KEY` — affected capabilities and restoration

**Effective behaviour.**
- SDK `@anthropic-ai/sdk` 0.90.0. Constructing a client without a key does **not** throw, so boot is unaffected. Each `messages.create` throws locally: `Could not resolve authentication method`. No network request is made.
- Every production caller goes through `utils/meteredAnthropic.js`. Before the call it **reserves** an `api_usage_events` row. After the local failure it settles that row as `status='uncertain'`, and `reserved_usd` is kept.
- Those phantom reservations count toward the `api_spend_policies` daily and monthly USD and request caps (`utils/apiSpendControl.js:102-105`).

**Unavailable while unset:**

| Capability | Entry point |
|---|---|
| Legacy Max chat (non-AO questions) | `POST /api/max/ask`. Returns an explicit `500 ANTHROPIC_API_KEY is not configured` (`routes/maxChat.js:328`) |
| Max daily briefing | `maxAgent.js` |
| Paige content and quality scoring | `paigeAgent.js` |
| Riley reply classification | `rileyAgent.js` |
| Vera, Rex, Sketch, Link, Faye, Ivy, Penny, reflection | respective agents |
| Pulse-health digest | `routes/cron.js` `handlePulseHealthCron` (`:598`) |
| Mira (Telegram) classification and anchor | `POST /telegram/mira`, `miraClassifierAgent.js`, `utils/miraAnchor.js` |
| Bland call-transcript parsing | `POST /webhooks/bland` (`routes/webhooks.js:850`) |
| Max interpretation shadow, if configured for anthropic | default-off, `MAX_INTERPRETATION_*` |

**Not affected:**
- Max workspace / AMO (`/api/v1/amo/*`): its reasoning path is deterministic, and the code there has no Anthropic call.
- The Max workspace presentation writer, which uses `GEMINI_API_KEY`.
- Auth, the dashboard and the setter/closer UIs.

**Restoration requirements (owner decision).**
1. Before you set the key, read the spend policy:
   `SELECT provider, paused, daily_usd_limit, monthly_usd_limit, daily_request_limit FROM api_spend_policies WHERE provider='anthropic';`
   A row with finite daily and monthly limits must exist. If there is no row, or a limit is NULL, the shared calculation is uncapped. Set limits first through the existing admin policy path.
2. Count the phantom reservations so you understand cap headroom:
   `SELECT agent_name, count(*), sum(reserved_usd) FROM api_usage_events WHERE provider='anthropic' AND status='uncertain' AND http_status IS NULL AND created_at > now() - interval '30 days' GROUP BY 1;`
   This is read-only. Do not delete these rows: they are the spend audit trail.
3. Paid calls start on restore even with workers and schedulers off. Inbound webhooks (`/telegram/mira`, `/webhooks/bland`), dashboard "run agent" buttons and legacy Max chat all call Claude directly. The Paige, Link, Faye and Vera approval flow still prevents anything being published.
4. The key's workspace must have access to the model ids hard-coded in the agents (for example `claude-sonnet-4-6` in the pulse-health digest).

**Verify.** Render Shell: `node -e "console.log(Boolean(process.env.ANTHROPIC_API_KEY))"` → `true`. Don't make a paid test call unless the owner approves one. If they do, ask legacy Max chat one question and confirm one `api_usage_events` row with `status='ok'`.

---

## 3. Unset safety flags — effective defaults

### `ANCHOR_RESEARCH_PROVIDERS_PAUSED` (unset → **unsafe**; set `true`)

- `utils/anchorResearchProviderPause.js:8`: `armed()` is `=== 'true'` exactly. Unset means the pause is **not armed**.
- So tenant-10 discovery, enrichment and verification handoffs to providers (Places, SerpAPI, Hunter, …) are blocked only by the pilot policy and spend caps, not by the pause.
- With workers and schedulers off, the remaining triggers are manual: `/api/run/scout`, a `CRON_SECRET` cron call, and Max/Scout actions in the workspace.
- Unset also **disables** every closed-state authority that requires `=== 'true'`: CRM preparation (`services/anchorCrmPreparationAuthority.js:36`), backlog recovery (`services/scoutBacklogRecovery.js:80`), bounded research (`services/scoutBoundedResearchAuthority.js:72`) and research-only grants (`services/scoutResearchOnlyGrant.js:85`).
- This is the worst combination: the safety pause is off, and the governed tools that assume the pause are unavailable.
- The operator runbook (`docs/runbooks/MAX-P0-002-OPERATOR-SECURITY-RUNBOOK.md` §8.1) requires `true`.

**Change.** `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`.

**Risks.**
- This is the documented SPEC-265 behaviour. While armed, a provider call with no tenant scope, or with conflicting tenants, is rejected for **any** tenant (`research_provider_tenant_required` / `_mismatch`).
- Correctly scoped non-10 calls are unchanged.
- Arming does not by itself enable research-only grants. They still need `ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true`, `SCOUT_RESEARCH_ONLY_GRANTS_ENABLED` and an installed grant.

**Verify.** Render Shell: `node -e "console.log(require('./utils/anchorResearchProviderPause').armed())"` → `true`. This only loads a pure module and makes no DB or provider call.

### `GOVERNED_OUTBOUND_TENANT_17_ENABLED` (unset → off; **no change**)

- `services/governedOutboundTenant.js:63-65`: tenant-17 sending is on only if `SUBSTRAL_GOVERNED_OUTBOUND_ENABLED === 'true'` **or** `GOVERNED_OUTBOUND_TENANT_17_ENABLED === 'true'`.
- Unset means sending is off. The schedule gate fails closed with `environment_kill_switch` (`services/governedTenantSchedule.js:20`), and the Substral governance blocks Emmett.
- Preparation-only for 17 needs **both** switches explicitly `'false'` plus `SUBSTRAL_GOVERNED_PREPARATION_ENABLED='true'` (`:77-79`). Unset therefore also keeps preparation off.
- Tenant 17 is not in the default `GOVERNED_OUTBOUND_TENANT_IDS` list either (`'10,13'`, `:43`).

Do **not** set it to `false` "for clarity". Explicit `false` on both switches is half of the preparation-enable condition.

**Verify.** Render Shell:
`node -e "const g=require('./services/governedOutboundTenant');console.log(g.governedOutboundEnabledForTenant('17'),g.governedOutboundPreparationEnabledForTenant('17'),g.parseGovernedOutboundTenantIds())"`
→ `false false [ '10', '13' ]`. If the list differs, `GOVERNED_OUTBOUND_TENANT_IDS` is set; confirm it is intended. Also confirm that `SUBSTRAL_GOVERNED_OUTBOUND_ENABLED` is not `true`.

---

## 4. `PF_MIGRATION_STAGING=0` — equivalent to unset

- `Dockerfile:30` CMD: `PF_MIGRATION_RECEIVER = "1"` → restore receiver; `elif PF_MIGRATION_STAGING = "1"` → staging probe; else `exec node server.js`. The comparisons are exact, so `0` routes to `server.js`.
- No JS reads the variable. The Python restore gateway treats anything other than `"1"` as disabled (`scripts/migration/restoreGatewayRender.py:428,495`), and the test `test/test_restoreGatewayContract.py:294` asserts `"0"` is disabled.
- This holds on two conditions, which you can check on the Render service settings page:
  - the service builds from `./Dockerfile`, not `Dockerfile.migration-render-restore`;
  - `PF_MIGRATION_RECEIVER` is not `1`.
- The runbook already requires `MIGRATION_RESTORE_APPROVED` and `MIGRATION_FINAL_SWAP_APPROVED` to be removed.

**Change (optional).** Delete `PF_MIGRATION_STAGING` in the same save. The cutover plan (`docs/migration/RAILWAY_EXIT_CUTOVER_PLAN.md:120`) specifies it unset, and deleting it does not change behaviour.

**Verify.** After the redeploy, the logs show `server.js` startup and no staging-probe or `node --test` output, and `/api/status` (authenticated) responds.

---

## 5. Bounded reconciliation plan — 20 unresolved attempts + 1 failed mailbox record

**Principles.**
- Classification is read-only. Unknown stays held. No row is deleted or reset.
- **No `--apply` within this package.** Releasing a `PROVEN_UNSENT` item clears it back to `pending` and makes the contact sendable again (`retryAllowed: true`).
- While unresolved, rows with `attempted_at IS NOT NULL` keep holding the per-email and per-company uniqueness (`acquisition_outbound_contact_once` / `_company_once`). That is the safe state. There is no urgency to resolve them.
- The P0-003 action ledger is not deployed, so `ledger.reconcile()` is not available. Use the deployed canonical reconciler, `services/governedUncertainSendReconciliation.js`.
- Don't touch any P0-004 table or code.

### 5.1 Inventory (read-only, Render Shell `psql`)

```sql
BEGIN READ ONLY;
SELECT i.tenant_id, i.id, i.status, i.reason, i.attempted_at,
       (i.provider_message_id IS NOT NULL) AS has_provider_id, e.mission_id, e.local_day
  FROM acquisition_outbound_items i
  JOIN acquisition_outbound_envelopes e ON e.id = i.envelope_id AND e.tenant_id = i.tenant_id
 WHERE i.status IN ('attempted','uncertain')
 ORDER BY i.tenant_id, i.attempted_at;

SELECT id, tenant_id, mission_id, prospect_id, thread_id, failure_code,
       (rfc_message_id IS NOT NULL) AS has_rfc_id, (provider_message_id IS NOT NULL) AS has_provider_id,
       metadata->>'providerAccepted' AS provider_accepted, created_at, updated_at
  FROM tenant_outreach_messages
 WHERE direction = 'OUTBOUND' AND status = 'failed';
ROLLBACK;
```

**Gate.** The counts must be 20 and 1, and they must be the same records as the earlier inspection. If not, stop and re-scope; don't extend the plan to new rows.

### 5.2 Per-item classification of the 20 (dry-run, DB-enforced read-only)

For each `(tenant_id, id)` from 5.1:

```sh
PGOPTIONS='-c default_transaction_read_only=on' \
  node scripts/reconcileGovernedUncertainFromEvidence.js --tenant <tenant_id> --item <id> --dry-run
```

- Dry-run returns before every write and takes no locks.
- `PGOPTIONS` makes Postgres reject any write. node-pg passes it through, which was checked against the installed `pg`.
- Tenant-10 items under recovery quarantine return `UNKNOWN` with no write.

Record each result in a table: item, tenant, outcome, reason.

| Outcome | Meaning | Disposition in this package | Later (separately approved) |
|---|---|---|---|
| `PROVEN_UNSENT` | Canonical evidence shows the attempt stopped before the provider boundary | Hold | `--apply` only as part of an approved outbound-restart decision for that tenant. It re-opens the contact. |
| `SKIP` `provider_message_id_present` / `item_event_provider_receipt` / `mailbox_message_sent` / `canonical_execution_sent` / `schedule_shows_send_path` | Evidence of a send | Hold. Treat as **sent** for any contact-frequency decision. | Operator `accepted` reconciliation with the provider id (`governedOutbound.reconcile`, admin/manager). There is no deployed CLI for this, so it needs an approved scripted call. |
| `SKIP` `spec252_schedule_exists` | A durable schedule owns it | Hold. The schedule must stay non-executing while outbound is off. | Reconcile the schedule first |
| `UNKNOWN` | Provider boundary may have been crossed | **Hold indefinitely** | Resolve only with external evidence: tenant-10 Brevo transactional log by recipient and timestamp; mailbox tenants' SMTP Sent folder by `rfc_message_id`; replies referencing it |

### 5.3 The failed mailbox record

- In deployed `services/tenantMailbox.js:1533-1551`, **any** exception writes `status='failed'` with `providerAccepted:false`. That includes an error after `transport.sendMail` returned, or a failure while saving the `sent` row.
- The record is therefore **not** proof of non-delivery. The definitive-failure classifier (`smtpFailureIsDefinitive`) is P0-003 code and is not deployed.
- Classify from `failure_code` / `failure_message`:
  - **Definitive not-sent** only for an SMTP reply ≥400 before DATA completed, `EAUTH`, `EENVELOPE`, `EDNS`, or a socket connect/DNS failure.
  - **Anything else is unknown.** That includes `ETIMEDOUT` after connect, a `CONN` command error, and DB errors.
- For unknown, search the tenant mailbox's Sent folder and any replies for the record's `rfc_message_id` (read-only IMAP, operator-run).
- If the record's prospect also has an item in 5.2, the canonical reconciler already treats a message row with `rfc_message_id` as `mailbox_message_sent` (`SKIP`). That is the conservative answer.
- No status change in this package.

### 5.4 Output

A single evidence table: 21 rows, each with outcome, reason, evidence pointer and recommended later disposition. Attach it to the MAX-P0-002 record. The expected end state of this package is **21 classified, 0 mutated**.

---

## 6. Execution order and verification

1. **Pre-read (Render dashboard, read-only).** Service builds from `./Dockerfile`; `PF_MIGRATION_RECEIVER` ≠ `1`; `PF_DISABLE_BACKGROUND_WORKERS=1`; `ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true` and `ANCHOR_GOVERNED_OUTBOUND_ENABLED=false` per runbook §8.1. Note the live deploy id and commit `69e7216`.
2. **Environment edit (one save).** Set `SESSION_SECRET` and `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`, delete `PF_MIGRATION_STAGING`, and add `ANTHROPIC_API_KEY` only if §2 is approved. Choose **"Save and deploy"**, which reuses the existing build. **Not** "Save, rebuild, and deploy", and **not** "Deploy latest commit": with auto-deploy off, branch head (≠ `69e7216`) must not ship.
3. **Confirm the deploy.** Render Events show the new deploy on commit `69e7216`. If any other commit appears, roll back to the noted deploy at once.
4. **Post-checks.**
   - §1 verify, §3 verify (both flags), §4 verify, §2 verify if restored.
   - Re-check that there are no advisory locks: `SELECT count(*) FROM pg_locks WHERE locktype='advisory';` → 0.
   - Confirm that no outbound, worker or provider activity started: no new rows in `api_usage_events` other than those from an approved test, and no new `acquisition_outbound_items` with status `attempted`.
5. **Then** run §5 (read-only) and file the evidence table.

**Rollback.**
- Env changes: revert in the Environment tab and redeploy the same commit.
- `SESSION_SECRET` rollback isn't recommended: it would re-open the fallback. Re-login is the only cost of going forward.
- The pause flag can be removed, but only on a documented decision.

**Out of scope (unchanged).** P0-003 migration, P0-004 work, workers, schedulers, outbound, deploys of new code, session-table purge, pgAdmin (C11).

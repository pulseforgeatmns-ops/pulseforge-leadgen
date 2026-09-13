# Anchor tenant 10 — Emmett outbound readiness (AUDIT)

Checked 2026-09-13. Stopped at the first real production blocker. No mail was sent. `autosend_enabled` was not changed. `enabled_agents` was not changed.

Production mission referenced by the Scout/PREPARE validation: `mission_1ddd1acb-6bae-4d51-baa8-ca449bab061a` (READY; Scout `qualifiedCount: 27`). This audit does not reopen Scout, Max, Paige, or PREPARE architecture.

## Verdict

| Field | State |
|---|---|
| Current sender identity | Seeded / code contract: `Jacob Maynard <jacob@goanchorcleaning.com>` on `goanchorcleaning.com`. Domain aligns. Live `clients` row not queried from this environment (`DATABASE_URL` unset). |
| Brevo readiness | DNS ownership + DKIM are live. Brevo API `verified`/`authenticated` and sender `active` **not confirmed** (no `BREVO_API_KEY` in this environment; Railway env not listed). Code still requires both API flags plus an active sender. |
| Missing config | Live confirmation of (1) Railway `BREVO_API_KEY` presence, (2) Brevo domain `{verified:true, authenticated:true}`, (3) sender `jacob@goanchorcleaning.com` `active:true`. Not the first blocker. |
| `enabled_agents` must change? | **No.** Canonical `EXECUTE_OUTBOUND` does not consult `enabled_agents`. That gate is only `/api/run/:agent` and `/cron/:agent`. |
| Current `autosend_enabled` | Schema default `false`. Seed for tenant 10 does not enable it. Enabling canonical Emmett does **not** autosend. |
| Canonical send path | READY → `APPROVE_EXECUTION` (no send) → explicit `EXECUTE_OUTBOUND` CER → TME → `executeOutboundBundle` → Brevo `POST /v3/smtp/email`. |
| Duplicate-send / idempotency | Intact in code: `executionIdentity = sha256(missionId:prospectId:revision)`, `idempotencyKey = exec_<32>`, skip if prior `SENT`, Brevo `Idempotency-Key` header. |
| **First real blocker** | **SPEC-212 message bindings are stripped when CAPACITY is persisted.** Live `EXECUTE_OUTBOUND` fail-closes with `tme_message_binding_contamination` / `missing_message_binding` on any non-empty queue. SPEC-071 tests hide this by re-injecting bindings after PREPARE. |
| **Exact next operator action** | Do **not** approve or execute outbound. Preserve `paige.candidateId`, `bindingScope`, and `attributableIntelligence` through `sanitizeQueueItem` (or an equivalent EXECUTE-side rebind from Paige VARIANTS **before** SPEC-212 validation). Then rerun the read-only probe on Railway. |

## 1. Sender identity

Canonical authority is `clients.sender_email` / `sender_name` / `sending_domain` only (`utils/canonicalSenderIdentity.js`, SPEC-186). Env `FROM_EMAIL` / `BREVO_SENDER_EMAIL` cannot execute tenant AMO sends.

Seed for `client_id=10` (`utils/clientContext.js`):

- `sender_email`: `jacob@goanchorcleaning.com`
- `sender_name`: `Jacob Maynard`
- `sending_domain`: `goanchorcleaning.com`

Email domain equals sending domain. Code blocks `canonical_sender_domain_mismatch` if that ever drifts.

Live row was not read here. PREPARE already ran against production `clients` for this tenant; CAPACITY `senderIdentity` is bound into the prepared-artifact revision.

## 2. Brevo readiness

### What code enforces

`evaluateSenderIdentityReadiness()` / `evaluateCanonicalSenderReadiness()` require:

1. `client_sender_configured`
2. `client_sender_domain_matches`
3. `brevo_domain_authenticated` — Brevo domain record `verified === true` **and** `authenticated === true`
4. `brevo_sender_active` — sender listed and `active === true`

DKIM/SPF/DMARC are **not** parsed from DNS by the send gate. They are inferred only via Brevo’s domain object. Inbox-health scoring uses snapshot `authentication` (boolean SPF/DKIM plus DMARC `none` for a configured domain) and can reduce capacity; it does not replace the Brevo API gate.

`getBrevoState()` is read-only: `GET /v3/senders/domains/{domain}` and `GET /v3/senders`. No send.

### Live DNS (2026-09-13) — no mail sent

June 28 docs (`docs/ANCHOR_BREVO_DNS.md`) are stale. Current public DNS:

| Record | Value |
|---|---|
| NS | `dns1.registrar-servers.com`, `dns2.registrar-servers.com` |
| TXT `@` | `brevo-code:bb22f3a79e20f15330ea1d92ad899bad` (domain **is** registered in a Brevo account) |
| TXT `@` | `v=spf1 include:_spf.google.com ~all` (unchanged Google SPF; do not add a second SPF) |
| `_dmarc` | `v=DMARC1; p=none; rua=mailto:jacob@goanchorcleaning.com` |
| `brevo1._domainkey` | CNAME → `b1.goanchorcleaning-com.dkim.brevo.com` (DKIM TXT published) |
| `brevo2._domainkey` | CNAME → `b2.goanchorcleaning-com.dkim.brevo.com` (DKIM TXT published) |

DNS proves Jacob added the domain and published Brevo ownership + DKIM. It does **not** prove Brevo flipped `authenticated: true` or that `jacob@goanchorcleaning.com` is an active sender.

### Railway `BREVO_API_KEY`

Not observed from this Cloud Agent VM (`BREVO_API_KEY` unset; Railway CLI / SSH unavailable). Pulseforge client-1 outbound historically uses the same key; that is inference, not a probe. Confirm with the read-only script on Railway.

## 3. Canonical Emmett execution

```
POST /api/v1/amo/missions/:id/execute
  → executeCanonical()
    → createExecutionRequest({ intent })
    → routeExecutionRequest()
      APPROVE_EXECUTION → advanceExecutionAfterApproval()   // writes approval; does not send
      EXECUTE_OUTBOUND  → advanceExecuteOutbound()
        → validateExecuteOutboundPreconditions()
        → executeOutboundBundle()
          → resolveCanonicalSenderIdentity()
          → evaluateCanonicalSenderReadiness()   // Brevo GET
          → buildExecutionBundle()
          → providers/brevo/sendEmail()          // only real send
```

Required before send:

| Requirement | Gate |
|---|---|
| Mission `READY` or `EXECUTE` | `tme_wrong_stage` |
| Operator `APPROVE_EXECUTION` matching current prepared-artifact revision | `tme_execution_not_approved` / `tme_execution_approval_stale` |
| CAPACITY + VARIANTS present (revision includes Max/Paige/Emmett IDs, queue targets, governor, sender email/domain) | stale approval if any change |
| Governor not pause/emergency | `tme_deliverability_paused` |
| SPEC-212 bindings valid on CAPACITY queue | `tme_message_binding_contamination` |
| Canonical sender complete + domain match | `canonical_sender_*` |
| Brevo domain authenticated + sender active | `canonical_sender_not_ready` |

`APPROVE_EXECUTION` clears `pendingOperatorDecision` and does **not** chain `EXECUTE_OUTBOUND`. A second explicit CER with `intent: EXECUTE_OUTBOUND` is required. Workspace approval of “Authorize execution?” is only the first click.

## 4. Tenant activation / `enabled_agents`

`isAgentEnabledForClient()` is used only by:

- `POST /api/run/:agent` (`routes/api.js`)
- `GET/POST /cron/:agent` (`routes/cron.js`) — cron `emmett` loads `emmettSchedulerCron` (autosend slice)

`ExecutionRouter` / `advanceExecuteOutbound` / `advanceEmmettCapacity` never call it.

**Do not add `emmett` to `enabled_agents` for canonical outbound.** Leave tenant 10 Scout-only until a deliberate legacy cron/manual Emmett decision.

## 5. Autosend

`utils/emmettAutosend.autorun()` returns `halted_reason: 'disabled'` unless **all** of:

1. `EMMETT_AUTOSEND_ENABLED` env is truthy
2. `clients.autosend_enabled === true`
3. `clients.active === true`

Canonical `EXECUTE_OUTBOUND` does not read `autosend_enabled`. Enabling AMO execute (or even adding `emmett` to `enabled_agents`) does not start autonomous sending.

The operator action that causes a **real** send is an authenticated `EXECUTE_OUTBOUND` CER after a valid execution approval (API, Mission Workspace execute control, or equivalent). Not `APPROVE_EXECUTION`. Not cron. Not PREPARE.

## 6. SPEC-212 — first blocker (proven locally)

`buildMissionBoundCandidates()` attaches `paige.candidateId`, `bindingScope`, `attributableIntelligence`, subject, and body.

`packages/emmett-outbound/Queue.js` copies that `paige` object onto queue items and sets `sendable` from subject+body.

`sanitizeQueueItem()` in `EmmettCapacityExecution.js` then keeps only:

```js
{ author, source, ready, variantLabel, sendable }
```

It drops `candidateId`, `bindingScope`, `attributableIntelligence`, `subject`, and `body`.

`validateProspectMessageBindings()` (EXECUTE precondition) then fail-closes: `missing_message_binding`.

Local proof (no DB, no Brevo, no send): after sanitize, `candidateId` is null and validation result is `contaminated`.

SPEC-071 (`spec071ExecuteOutbound.test.js` `throughExecutionApproved`) **patches the in-memory CAPACITY row after PREPARE** to put bindings back. Production does not.

Until CAPACITY persist preserves SPEC-212 fields (or EXECUTE rebinds from Paige VARIANTS before that validator), one operator-approved outbound send cannot complete.

Idempotency after that fix is already implemented and does not need activation work.

## 7. Safe production probe

This environment: `DATABASE_URL` and `BREVO_API_KEY` unset. Performed here:

- Code-path trace of EXECUTE
- Live DNS for `goanchorcleaning.com`
- Local SPEC-212 sanitize proof
- No `POST /v3/smtp/email`
- No fixture recipients
- No `autosend_enabled` write
- No `enabled_agents` write

On Railway (read-only):

```bash
cd /app && node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production
```

The script only SELECTs tenant 10 / READY missions and GETs Brevo senders/domains.

## Minimal activation steps (after the SPEC-212 fix)

Do these only after bindings survive CAPACITY persist and the Railway probe prints `spec212.valid=true` and `senderReadiness.sendable=true`:

1. Keep `autosend_enabled = false`. Do not set `EMMETT_AUTOSEND_ENABLED`.
2. Do not change `enabled_agents`.
3. Open the READY Anchor mission (`mission_1ddd1acb-6bae-4d51-baa8-ca449bab061a` or a successor at READY).
4. Operator: `APPROVE_EXECUTION` (review queue; this still does not send).
5. Operator: explicit `EXECUTE_OUTBOUND` for **one** approved queue item (or re-prepare a one-item queue first).
6. Confirm one Brevo `SENT` execution record and webhook correlation. No second send of the same `executionIdentity`.

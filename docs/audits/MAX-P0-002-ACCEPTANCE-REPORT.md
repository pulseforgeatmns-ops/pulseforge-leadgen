# MAX-P0-002 — Final reconciliation and acceptance report

Status: **configuration accepted; record reconciliation prepared and verified, awaiting one read-only Shell readback.**
Deployed code: Render, commit `69e7216`. Assessed 2026-11-27.

No mutation, release, send, deploy, migration or worker activation was performed or is requested here.

---

## 1. Why the reconciliation was not executed directly

- The production PostgreSQL (`pulseforge-production-migration`, PG 18) has an **empty IP allowlist**, so it refuses every external connection, including Render's read-only query tool.
- That allowlist is a security control and was deliberately left unchanged.
- The reconciliation is therefore delivered as one read-only paste block to run in the Render Shell: `docs/audits/MAX-P0-002-RECONCILIATION-SHELL.md`.
- The block was executed end to end against a disposable PostgreSQL built from the deployed migrations, with fixtures covering each outcome path. It wrote nothing.

## 2. Correction to the earlier remediation plan (§5 of `MAX-P0-002-CONFIGURATION-REMEDIATION.md`)

- The 20 "unresolved outbound attempts" are the P0-003 pre-migration check's §4c population: **AMO `acquisition_mission_outbound_executions` rows with `status='attempted'` and no `sent`/`failed` row for the same `execution_identity`**.
- They are **not** governed-outbound `acquisition_outbound_items`.
- The governed reconciler (`scripts/reconcileGovernedUncertainFromEvidence.js`) does not apply to them, and there is no deployed reconciler for this table.
- The reconciliation below therefore classifies from evidence with read-only SQL and makes no release.

**What an `attempted` AMO row means in the deployed code.**
- `packages/max/workspace/OutboundExecutionAdapter.js:250-293` writes `attempted`, calls the provider, then writes `sent`/`failed`.
- The Brevo `sendEmail` never throws (`packages/providers/brevo/sendEmail.js`). So a row left at `attempted` means the process stopped, or the final write failed, between the two writes. That can happen before or after Brevo accepted the request.
- No attempt token links the row to a pre-provider proof. **An `attempted` AMO row can therefore never be proven unsent from local data.** It can only be confirmed sent from direct evidence, or stay `UNKNOWN`.

## 3. Classification rules applied

| Outcome | AMO attempts (20) | Failed mailbox record (1) |
|---|---|---|
| `SENT_CONFIRMED` | Provider events stored against this record id; provider message id or provider response on the record; **or** (Brevo step) a Brevo event whose tags carry exactly `mission:<id>` and `prospect:<id>` (and `revision:<rev>` if Brevo shows one) | Provider id or `sent_at` on the record |
| `NOT_SENT_DEFINITIVE` | **Not available**: no pre-boundary proof exists for this table | Authorization refused before dispatch (`outreach_review_required`, `CALL_SUPPRESSED`, `tenant_pilot_policy_denied`, `anchor_recovery_scope_invalid`); `EAUTH`/`EENVELOPE`/`EDNS`; socket `connect`/`getaddrinfo` failure; `EMESSAGE` with a 4xx/5xx SMTP reply |
| `UNKNOWN` | Everything else, with the sub-reason recorded: `governed_attempt_crossed_provider_boundary`, `unattributed_send_signal_near_attempt`, `governed_pre_provider_signal_not_linked_to_record`, `no_local_post_attempt_evidence`; from Brevo `no_brevo_event_found_in_window_not_proof` | Everything else, including `ECONNECTION`, `ESOCKET`, `ETIMEDOUT`, `CONN` and generic `smtp_send_failed`. The deployed mailbox code marks *any* exception `failed`, even after `sendMail` returned. |

- Unattributed signals (CRM touchpoints, governed items near the same time, opens) are reported but never upgrade a record.
- Absence of a Brevo event is never proof: retention limits and filters apply.

## 4. Acceptance criteria and readback

P0-002 is **accepted** when the Shell output shows all of the following.

| Check | Section | Required |
|---|---|---|
| Session read-only | 0 | `read_only = on` |
| Fail-closed precondition | 1a | `P0002_PRECONDITION_OK` (otherwise psql stops before classification and no Brevo lookup is made) |
| Baseline unchanged | 1 | `unresolved_attempts = 20`, `duplicate_groups = 0`, `failed_mailbox = 1`, P0-003 tables `f`, `advisory_locks = 0` |
| Quiescent | 2 | all 24h write counters `0`, `live_schedules = 0` (any non-zero value must be explained before acceptance) |
| Every record classified | 3, 3b, 5 | 20 + 1 rows, each with outcome and reason |
| Holds intact | 6 | `attempted_rows_still_attempted = 20`, no pending governed item or live schedule for an affected prospect |
| All required queries succeeded, nothing written | footer | `P0002_SQL_COMPLETE …` then `"status": "P0002_BREVO_COMPLETE"` and `brevo_exit=0`. `P0002_SQL_FAILED` means no acceptance. Optional sections may print `UNAVAILABLE`. |

- Acceptance does **not** require resolving `UNKNOWN`s. Unknowns stay held indefinitely.
- If section 1 differs from the baseline, stop: the population changed and must be re-scoped. Do not classify new rows under this package.

**Results table (to be filled from the readback; nothing below is assumed):**

| # | Record | Tenant | Local outcome / reason | Brevo outcome / reason | Final |
|---|---|---|---|---|---|
| 1–20 | AMO attempts | | | | |
| 21 | failed mailbox message | | | n/a | |

Final per record = `SENT_CONFIRMED` if either source confirms it; `NOT_SENT_DEFINITIVE` (mailbox only) per §3; otherwise `UNKNOWN`.

## 5. Disposition (unchanged by this package)

- **All 21 records stay exactly as they are.** No status changes and no releases.
- `SENT_CONFIRMED`: treat as contacted for every frequency/R7 decision. Correcting the stored `status` to `sent` is a later, separately approved write.
- `UNKNOWN`: held. Treat as **possibly sent**: no follow-up and no re-send to that prospect under the same mission. This matches P0-003 R7 ("pre-ledger ATTEMPTED records … treated as outcome unknown, never re-sent").
- `NOT_SENT_DEFINITIVE` (mailbox): eligible for a later, approved retry decision only. Not part of P0-002.

## 6. Configuration acceptance (confirmed by operator)

| Item | State |
|---|---|
| `SESSION_SECRET` | Set, length verified. The repo fallback is no longer effective. |
| `ANCHOR_RESEARCH_PROVIDERS_PAUSED` | `true`, verified |
| Background workers / external schedulers / governed outbound | Disabled |
| `GOVERNED_OUTBOUND_TENANT_17_ENABLED` | Unset = off (no change by design) |
| `PF_MIGRATION_STAGING` | `0` ≡ unset for runtime routing |
| Live deploy | `69e7216`, independently confirmed |
| Anthropic | **Retired by owner decision.** Credentials will not be restored and no spending policy will be configured. |

**Residual effects of the Anthropic retirement in the deployed code.**
- These code paths still exist and fail on each call: Paige authoring/scoring, Riley classification, Max briefing, legacy Max chat (500), Vera/Rex/Sketch/Link/Faye/Ivy/Penny, pulse-health digest, Mira, and Bland transcript parsing.
- Each attempt also leaves an `api_usage_events` row with `status='uncertain'` whose reserved USD counts toward shared caps.
- Max workspace/AMO and the Gemini presentation writer are unaffected.

## 7. Remaining activation prerequisites for P0-006

No P0-006 specification exists in the repository (this branch or `main`). These prerequisites are **derived from P0-002 findings** and must be reconciled with the P0-006 spec when it is written.

1. **P0-002 readback accepted** per §4, with the 21-row table filed.
2. **Reliability code actually deployed.**
   - `69e7216` contains neither MAX-P0-003 (mission lease, CAS, external-action ledger, `Idempotency-Key`, mailbox ledger, definitive-SMTP rule) nor MAX-P0-004.
   - Before any outbound or mission execution: apply `migrations/2026-11-25-max-p0-003-reliability-core.sql` (pre-check: `scripts/max-reliability/pre-migration-check-readonly.sql`), then the P0-004 migration. Both are separately approved.
   - Then deploy a reviewed commit that contains both, with durable modes default-off. Without the P0-003 tables, sends fail closed (`reliability_schema_missing`).
3. **Unknown outcomes carried into the ledger era.** All `UNKNOWN` AMO attempts and the mailbox record must remain blocking for their prospects. Verify that the deployed R7 rule (`outreach_intent_already_dispatched`) sees them before any send path is enabled.
4. **Anthropic-dependent capabilities replaced or explicitly disabled.**
   - Outbound copy is authored by Paige (Claude) and reply triage by Riley (Claude). An outbound activation needs a non-Anthropic authoring path or human-authored copy, and a working reply/unsubscribe path. Verify which classifier the governed mailbox poll uses.
   - Callers that would only accumulate `uncertain` spend reservations should be disabled.
5. **Governed outbound per tenant.** Program authorization renewed (Anchor's expired 2026-11-09). Maintenance/kill-switch state read back. Mailbox credentials valid. `GOVERNED_OUTBOUND_TENANT_IDS` and per-tenant switches set deliberately: unset defaults to `10,13`, and the tenant-13 clock arms if `BABRUN_GOVERNED_OUTBOUND_ENABLED=true`.
6. **Single bounded canary** (`maxSends: 1`) with provider-id readback before any recurring scheduler. Workers stay off until then.
7. **Open from earlier P0-002 findings (unchanged):**
   - publicly reachable pgAdmin (C11): owner decision;
   - the Railway → Render variable name-by-name reconciliation (O6);
   - removal of `MIGRATION_RESTORE_APPROVED` / `MIGRATION_FINAL_SWAP_APPROVED` if still present.

## 8. Boundaries respected

- No production write, release, send, deploy, migration or worker change.
- The database allowlist was not modified.
- No P0-004 file was touched.
- Repository changes are limited to:
  - `scripts/max-p0-002/` (read-only tools, not wired into the app);
  - `docs/audits/MAX-P0-002-RECONCILIATION-SHELL.md`;
  - this report;
  - the plan correction note.

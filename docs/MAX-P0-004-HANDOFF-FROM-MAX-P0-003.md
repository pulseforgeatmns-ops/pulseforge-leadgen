# Handoff to the MAX-P0-004 session (from the MAX-P0-003 review)

This session does **not** implement MAX-P0-004 and has created no MAX-P0-004 branch. This note gives the active MAX-P0-004 session everything from the MAX-P0-003 review that its design must respect.

## 1. Branch and state

| Item | Value |
|---|---|
| PR | #1163 (draft → returned for explicit merge approval) |
| Branch | `ccr-f987a12e-t5jhov` |
| Base | `main` @ `3bc3360` (includes #1146 reconciliation and #1162 MAX-P0-003 core) |
| Head SHA | see the PR's latest commit; recorded in the final report of this session |
| Production | Render `pulseforge-api-migration` @ `69e7216`, branch `migration/render-current-main`, auto-deploy off. **Nothing from #1162/#1163 is deployed.** |
| Migration | `migrations/2026-11-25-max-p0-003-reliability-core.sql`: **not applied anywhere.** |

Build MAX-P0-004 on `main` after #1163 merges, or rebase onto it. #1163 changes the interfaces below.

## 2. Files and interfaces changed by #1163

| File | Interface / behaviour |
|---|---|
| `services/missionLease.js` | **New exports** `MISSION_SESSION_LOCK_SQL`, `MISSION_SESSION_UNLOCK_SQL`: a session-level lock on the **same key** as the transaction-scoped mission lease (`hashtextextended('amo-mission:'||id, 0)`), for code that already owns ONE dedicated connection. Lease and session holders exclude each other. `missionLockText(id)` gives the key text. |
| `services/governedOutboundAdapters.js` | Anchor recovery approval now holds `MISSION_SESSION_LOCK_SQL` (not the old ADR-075 two-int key 750825) on its dedicated `persistencePool` connection. The nested TME stage takes no second lease because the wrapper has no `connect()`. |
| `services/maxReliability/externalActionLedger.js` | **New** `findBlockingForSubject({ tenantId, actionKind, missionId, subjectRef, excludeKey })`: the latest claim for the same logical target under another key in a state that may have reached the recipient. |
| `packages/max/workspace/OutboundExecutionAdapter.js` | R7: refuses `outreach_intent_already_dispatched` when the same (tenant, mission, prospect) already has a blocking claim or a `SENT`/`ATTEMPTED` record under another revision. Exports `findPriorDispatchForProspect`. |
| `services/tenantMailbox.js` | `sendTenantEmail` (PostgreSQL store) claims in the ledger; new export `smtpFailureIsDefinitive(err)`; option `opts.externalActionLedger` (`null` opts out — test doubles only). |
| `routes/acquisitionMissions.js` | `/execute`: 428 `idempotency_key_required` for external-effect intents; permanent receipt = `executeReceipt(body)` allowlist; `failStatus()` maps lease/CAS errors to 409/404. |
| `services/maxReliability/keyedRequest.js` | **New option** `receiptOf(body)`: the permanent receipt is the caller's allowlisted summary. |
| `services/maxReliability/executionRecovery.js` | `recoverInterruptedExecution(pool, { tenantId, apply, actor, … })`: `apply` requires `actor` (`recovery_actor_required`); writes a `recovery_applied` audit row. |
| `scripts/max-reliability/recover.cjs` | `--apply` requires `--confirm-tenant <id>` (= `--tenant`) and `--operator <id>`. |
| `utils/workerAdvisoryLock.js` | **New** `withWorkerAdvisoryLock(pool, key, fn, { label })`: lock, work and unlock on one dedicated connection; destroys the connection if the unlock fails. Used by `miraTranscriptionAgent`, `miraClassifierAgent`, `miraRouterAgent`, `warmRoutingAgent`. |
| `public/acquisition-missions.html` | Sends `Idempotency-Key: ui-<uuid>` per click; ignores a second click while one is in flight. |
| `migrations/2026-11-25-max-p0-003-reliability-core.sql` | Adds triggers `max_reliability_forbid_delete()` / `max_reliability_receipt_delete_guard()`. |
| `.github/workflows/penny-anchor-daily.yml`, `test/ciPlanPr.test.js` | Penny schedule paused (manual dispatch only), with a CI guard. Levi stays manual-only. |

## 3. Requirements MAX-P0-004 must preserve

### 3.1 Idempotency and ownership
1. External-effect execution requests carry a client idempotency key. REST already returns 428 without one. When MAX-P0-004 adds chat/task identity, **chat must also carry a client message id** feeding the same receipt → task chain. Do not reintroduce keyless external-effect entry points.
2. A receipt is permanent and maps 1:1 to the root task (proposal §3.7). A replay must return the original receipt plus live task state, and **never re-run**. `interrupted` receipts answer `request_outcome_unknown`.
3. Permanent receipts store an **allowlisted summary** only (use `receiptOf`). Never store operator free text, payloads, snapshots or recipients in receipts, claims or audit rows.
4. Mission mutual exclusion has one key: `missionLockText(id)`. Every mission-writing critical section — task owner commit, recovery, scripts — must contend on it: via `acquireMissionLease` (pool) or `MISSION_SESSION_LOCK_SQL` (dedicated connection). Never take a session lock through `pool.query`. Never revive the 750825 two-int key.
5. If MAX-P0-004 narrows the mission lease to the commit (proposal §3.2), the task **fence check must be inside the same lease transaction as the mission write**. Keep the multi-process PostgreSQL tests (`test/maxReliabilityCorePostgres.test.js`) green, and add fence-specific ones.
6. Durable CAS on missions stays: `persistMission` refuses tenant mismatch and stale writers, and both fingerprint sides read the JSON payload, never the `updated_at` column.

### 3.2 External actions (claims)
7. Every irreversible provider call goes claim → committed dispatch marker → provider (`guardedDispatch`, or the mailbox pattern below). The marker is committed on its own autocommit connection, never inside a business transaction.
8. `outcome_unknown` and `dispatch_attempted` are never auto-retried. A task with a claim in doubt ends `awaiting_intervention`, never `interrupted`/retried.
9. R7: one outbound action per (tenant, mission, prospect). A content or revision change must not bypass an existing claim, an unresolved outcome, suppression or duplicate restrictions. Follow-up outreach is a **distinct, separately authorized logical intent**. MAX-P0-004 must not add a path that creates one implicitly.
10. Claims are linked to tasks via a nullable `task_id` column (an additive migration in MAX-P0-004). A task cannot complete while any of its claims is `claimed`/`dispatch_attempted`/`outcome_unknown`.
11. Not yet on the ledger, and in MAX-P0-004 scope: Paige social publishing (`packages/capabilities/contentPublication/adapters/SocialPlatformAdapters.js`) and Twilio/Bland senders.

### 3.3 Mailbox ledger integration (reuse this pattern for other transports)
- Key `tenant-mailbox:<outboundIdempotencyKey(input)>`, action kind `tenant_mailbox_smtp`. Fingerprint: tenant, sending identity, sorted lowercased recipients, subject, text/body, html, In-Reply-To.
- Order: **claim → `PENDING` row → (authorization wrapper) → marker committed inside the innermost `dispatch()` immediately before `sendMail` → result**. An authorization refusal or a failed `PENDING` write releases the claim (`abandoned_not_dispatched`) without a false dispatch.
- `smtpFailureIsDefinitive(err)`: true for an SMTP reply ≥400, `EAUTH`/`EENVELOPE`/`EDNS`, `ECONNECTION` in the `CONN` phase, `syscall` connect/getaddrinfo, or a "connect E…" message. Everything else after the marker is unknown (row: `failure_code = external_action_outcome_unknown`, `metadata.providerAccepted = null`).
- Replay of an acknowledged claim restores the message row to `SENT` from `provider_ref`, with no SMTP call.
- The in-memory store is a test transport and is not ledgered.

### 3.4 Advisory locks
- Single-flight workers: `withWorkerAdvisoryLock`.
- Still to migrate to destroy-on-unlock-failure (MAX-P0-004 scope): governed outbound store, replies and control dispatch; tenant outreach executor lock and scheduler; Emmett autosend. These are currently safe (dedicated client) but return the client to the pool if unlock throws.
- Any new task-claim lock must follow the same one-connection rule.

### 3.5 Recovery and receipt retention
- Recovery is operator-run, tenant-scoped, plan-by-default, and requires `--operator` and `--confirm-tenant`. It is audited. It never dispatches, retries or reconciles.
- MAX-P0-004 task recovery should extend `recover.cjs` (one audit trail), not add a second tool.
- `DELETE`/`TRUNCATE` is forbidden by trigger on claims, audit and non-`in_progress` receipts. MAX-P0-004's `max_task_events` should get the same trigger. Erasure is by `UPDATE` scrub, never row deletion of dispatched claims. Archival needs a separately approved migration (`docs/MAX-P0-003-REVIEW.md` §6).
- Recovery must not run on boot while `PF_DISABLE_BACKGROUND_WORKERS=1`.

### 3.6 Testing constraints
- PostgreSQL integration tests must use disposable PostgreSQL. They cannot run as root (run as an unprivileged user locally).
- Use `createTrackedPostgresPool` + `closePostgresFixture`. Raw `pool.end(); pg.stop()` races at teardown (L2).
- Known load-sensitive: `anchorDailyOutboundSupervisedWorker` (L1, 200 ms healthy-worker deadline). Pre-existing; fails safe.
- `test:amo` is gated by the ratchet (`scripts/ci/testRatchet.js --baseline test/baselines/amo-known-failures.json …`). Add no baseline exceptions.

## 4. Architecture documentation
- `docs/MAX-P0-003-RELIABILITY-CORE.md`: lease, CAS, ledger, receipts, recovery, diagrams, register.
- `docs/MAX-P0-003-REVIEW.md`: R1–R7, path matrix, crash-boundary table, lock audit, load register, operator checklist, retention verification.
- `docs/MAX-P0-004-DURABLE-EXECUTION-OWNERSHIP.md`: this session's proposal (input only; the active MAX-P0-004 session owns the architecture).
- `docs/reliability/durable-stage-scheduling.md`: the existing claim/start/fence/lease/quarantine pattern for governed outbound stages.
- `CLAUDE.md` → "Reliability core (MAX-P0-003, Nov 2026)".

## 5. Open items not in #1163
- `max-context-cleanup-hourly` stays scheduled. It is verified internal maintenance: one bounded `DELETE` of expired `max_conversation_context` rows, no specialists or external calls.
- Render deploy branch is behind `main`. Choose a single authoritative deploy source before any deploy.
- Production schema report: awaiting the operator's Render Shell output.

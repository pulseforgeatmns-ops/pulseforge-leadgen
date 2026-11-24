# MAX-P0-001 — Max Production Readiness Audit

**Audit date:** 2026-11-24 (UTC). **Mode:** read-only. No fix, configuration change, deploy, restart, migration, mission action, Scout run or outbound activity was performed.

**Deployed baseline:** Render service `pulseforge-api-migration` (`srv-dbtgtp3bc2fs73f5ukr0`), deploy `dep-dc2p9obm8hqs7386utd0`, commit `357e747` (PR #1159 merge), live since 2026-11-24 13:46:13Z. Deploy branch: `migration/render-current-main`, auto-deploy **off**, manual trigger.
**Repository `main`:** `76a1059`. It is 18 commits **behind** the deployed branch (and 2 commits ahead, both migration-only).

**Record correction (2026-11-24):** the commit that added this audit (`5f26c0d`) carries the trailer `Claude-Session: …/session_01WVeSoTsedqgJC9epZVU4RD`, which is wrong (copied from an earlier commit). The session that produced this audit is `https://claude.ai/code/session_01WVeSoVdPwRaWomyMo2zihK`. History was not rewritten to fix the trailer; this note is the correction of record.

**Evidence sources used:** repository at the deployed commit (read-only worktree); Render service, deploy, event, log and metric APIs (read only); GitHub Actions run history and job logs; local test runs (offline harness, no `DATABASE_URL`, no model key); one disposable local PostgreSQL 16 proof.

**Not reachable:** the production database (the Render Postgres IP allowlist is empty, so read-only SQL is refused), Render environment-variable values, Render HTTP request logs (only `app` and `build` log types are exposed), and any interactive Max session. Every capability that depends on these is marked **UNVERIFIED**.

---

## 1. Executive summary

Max is **not production-ready**, and its production behaviour since the Railway → Render cutover (2026-11-22) is largely **unverified**. Nothing observed suggests an unauthorized send or a cross-tenant disclosure. The gaps are of three kinds.

1. **The production environment is degraded.** These are confirmed from Render logs and Actions runs:
   - all background workers are disabled;
   - the hourly Max context-cleanup job has failed with HTTP 401 on every run inspected;
   - Paige fails every day with the Anthropic SDK's "no API key" error;
   - Riley has no Gmail credentials;
   - Max decay has no scheduled caller.

   Max's LLM-backed paths are probably affected by the same missing credential. That is inferred, not observed.
2. **Max's execution layer has no durable ownership of work.** Specialist dispatch is synchronous inside the HTTP request. There is no task ledger with claims, timeouts or abandonment detection, and the durable scheduling design is local-only. A concurrency defect in the mission advisory lock was **reproduced locally**: the lock leaks across pooled connections.
3. **Verification and CI evidence is weaker than it looks.**
   - The Max unit lane passes (2,925/2,925), but the acquisition-mission suite (`test:amo`), which holds the outbound-verification, replay and durability tests, is in **no** CI lane, and 29 of its tests fail on the deployed commit.
   - Full regression runs only on `main`, never on the deployed branch.
   - Two commits with **failing** PR CI were deployed to production.
   - The outcome-verification model is real, but it classifies what the handler reports rather than independently reading back the result. Its highest level is "provider accepted"; nothing in Max verifies effectiveness.

**Gate verdicts:** A PARTIAL · B PARTIAL · C **FAIL** · D PARTIAL · E **FAIL** · F PARTIAL. No gate can be graded PASS in production, because no Max turn, mission execution or restart has been observed on Render.

**Recommendation:** close P0 in this order:
1. production-truth and config reconciliation (operations; no feature work);
2. two small safety fixes (fail-closed cron auth, advisory lock);
3. restore CI truth on the deployed branch;
4. outbound verification and routing defects;
5. silent-failure alerting;
6. a Render-native acceptance run with a restart;
7. a bounded observation window.

§8 sets out the details and dependencies.

---

## 2. Canonical roadmap position

| Source | Position |
|---|---|
| `docs/vision/Product_Roadmap.md` | Version line ends at v1.0 "Intelligence-first operating partner". "Max orchestration flag graduation (shadow → limited write)" is a parallel track, not complete. |
| `docs/roadmap/MAX-MISSION-LIFECYCLE-HYGIENE.md` (2026-11-08) | Approved for the roadmap, **not authorized** during the P0 freeze. Step one is "restore and verify Max tenant context, routing, execution and evidence persistence". |
| `docs/P0-MAX-PRODUCTION-ACCEPTANCE.md` | Procedure S1–S9 is "prepared, not executed". It is still written for Railway (`shinkansen.proxy.rlwy.net`, Railway auto-deploy of `main`), so it is stale for Render. |
| `docs/P0-MAX-PRODUCTION-ACCEPTANCE-REPORT.md` (2026-11-09, Railway) | Semantic routing prohibited. JEV persistence failing (`schema_column_missing`). Continuity is a foundation only. Model-backed evaluation not run. |
| `docs/P0-MAX-ACCEPTANCE-ISSUE-REGISTER.md` | R1–R10 and P1–P4 open. Re-verified below. |
| `CLAUDE.md` | Still describes Railway as the deploy target ("Deployed on Railway"). Stale. |

**Position:** Max is inside the P0 reliability freeze, at the "restore and verify" step. That step was never completed on Railway, and the Render cutover reset the production baseline. The roadmap has no executive directive document beyond the files above. "Canonical roadmap" is therefore taken to mean these files.

---

## 3. Current Max architecture and execution lifecycle

```
Browser (Command Deck)
  → POST /api/v1/max/workspace/ask            routes/maxWorkspace.js:109
      requireAuth + requireRole(admin|manager|viewer|client)   :33-36
      tenant = session.active_client_id | users.client_id (client role locked)   TenantContextResolver.js
      review navigation → API-usage → client lock (SPEC-115) → session/tenant check :180-213
  → WorkspaceEngine.ask()                      packages/max/workspace/WorkspaceEngine.js:646
      shadows (JEV, semantic) — default-off, non-authoritative
      _askProduction()  :778
        session: in-memory SessionStore (2 h TTL, 1,024 max)  SessionStore.js:14
        owners in order: review nav → operational inspection (#1087) → evidence continuity (#1089)
          → objective resolution → pending decisions → AMO/mission → operational read (#1110) → reasoning
      record presented decision / ledger / continuity (default-off)
  → Mission execution: Canonical Execution Request → routeExecutionRequest()
      packages/acquisition-mission/ExecutionRouter.js (pilot policy, constraints, runtime ownership)
      → TransactionalExecution (TME): stage staged in memory → specialist handler (Scout/Paige/Emmett)
        → persistStageCommit (one PG transaction + advisory lock)  services/acquisitionMissionPersistence.js:935
      → classifyExecutionEvidence()  ExecutionEvidence.js (13 states; 3 "reportable as done")
```

**Lifecycle as implemented:**

| Concept | Implemented as |
|---|---|
| Planned | Mission plan or a pending operator decision in `acquisition_missions.payload` |
| Attempted | `dispatched` / `outcome_unknown` (in-request only; never durable) |
| Executed | `committed_internal`: TME commit with `transactionId` |
| Verified | `completed_verified`: outbound needs `status:'sent'` + `providerMessageId` on every record; discovery needs a live (non-synthetic) result |
| Effective | **Not modelled in Max.** Opens, replies and meetings arrive via Brevo webhooks / Riley into `email_performance` / touchpoints. They are not correlated back to the execution record. |

---

## 4. Acceptance gate assessment

### Gate A — Context assembly: **PARTIAL**

| Capability | Finding | Evidence |
|---|---|---|
| Tenant resolution | Fail-closed, never defaults to tenant 1. Client role is pinned to `users.client_id`. Body or context tenant mismatch → 403. | `routes/maxWorkspace.js:51-57, 180-213`; `TenantContextResolver.js` `resolveActiveTenantId` |
| Session ↔ tenant binding | An existing session belonging to another tenant → 403. Recovery checks the tenant. Sessions are bound to a tenant, **not to a user**: any user of the same tenant who holds a session UUID can continue it. | `routes/maxWorkspace.js:195-213`; `WorkspaceEngine.js:573-578` |
| Business context retrieval | Operating evidence, record intelligence and Blueprint paths exist and are unit-tested. | unit lane, 2,925 pass |
| Operational memory | In-memory only by default. The durable context is `MAX_CONVERSATION_CONTEXT`, whose production value is **UNVERIFIED** (env values are not readable). | `SessionStore.js:14`; `ConversationContinuity.js:6` |
| Historical decisions / active objectives | Durable in PostgreSQL (`acquisition_missions`, contributions, execution audit, authorization constraints). | `acquisitionMissionPersistence.js:44-234` |
| Permission boundaries | Monotonic authorization constraints, amendment step-up, tenant-21 pilot policy. | CLAUDE.md SPEC-MAX-RELIABILITY-004 §5/§8; `utils/tenantPilotPolicy.js` |
| Freshness | Evidence older than 30 min is re-read. Recovered referents require revalidation. | `OperationalEvidenceContinuity.js`; `WorkspaceEngine.js:620-631` |
| **Production** | No Max workspace turn was found in Render app logs since the cutover (searched 2026-11-22 19:30Z → 2026-11-24 18:40Z). LLM credential probably absent (§7 R-3). | Render logs |

### Gate B — Intent and routing: **PARTIAL**

| Capability | Finding | Evidence |
|---|---|---|
| Negation / authorization polarity | 270 safety regressions passed on the Railway commit (prior audit). The current unit lane is green. | prior report; `max-unit.log`: 2,925 pass / 0 fail |
| Discovery wording approves a plan (R2) | **Still open.** "Begin discovery." approves a pending plan. | `spec135.test.js`: 1 fail on `357e747` |
| Runtime boundary in `Scout.discover` (R3) | **Still open.** Cross-runtime sync is not rejected. | `spec170MissionRuntimeOwnership.test.js`: 1 fail |
| Autonomous chaining (R7) / explanation text (R10) | **Still open.** | `spec157…`: 1 fail; `test/acquisitionMission.test.js`: 2 fail |
| Bare "approved" on a mission with no pending decision (R9) | Not retested. No fix is found in the commit log. | register |
| Gate-bound approvals | Implemented, **default-off** (`MAX_GATE_BOUND_APPROVAL`). Production value UNVERIFIED. | `GateBoundApproval.js:40` |
| Semantic accuracy | Historical: 24% overall / 98% safety (generalization), 53.2% lexical; foreign-tenant recognition 0%. Model-backed evaluation never run. | prior report |
| Cross-tenant isolation | Route-level plus router-level (mission tenant). Pilot policy refuses tenant-21 dispatch even when referenced under another tenant. | `tenantPilotPolicyDispatchPostgres` (per readiness doc); route code |
| Viewer role | `viewer` may call `/workspace/ask`. Whether a viewer can drive mission approvals through chat is **UNVERIFIED** (not traced in this audit). | `routes/maxWorkspace.js:33-36` |

### Gate C — Execution ownership: **FAIL**

| Capability | Finding | Evidence |
|---|---|---|
| Durable task creation | Missions and stage commits are durable. There is **no durable task or delegation record** for in-flight specialist work: dispatch runs inside the HTTP request, and `running` is deliberately never durable (SPEC-131). | `ExecutionEvidence.js` header; `TransactionalExecution.js` |
| Delegation and tracking | Execution audit rows are written only on commit. A process death mid-stage leaves no record of the attempt, apart from provider-side receipts for governed outbound. | `persistStageCommit` |
| Dependencies / async completion | No asynchronous completion model for Max-delegated work. Durable stage scheduling (claims, fences, leases, quarantine) is **local implementation only**. | `docs/reliability/durable-stage-scheduling.md` ("LOCAL IMPLEMENTATION ONLY") |
| Abandoned-task prevention | No stalled-mission or stalled-decision detector in production. Lifecycle hygiene is unauthorized. `/cron/max-attention-cycle` exists, but no caller was found (no workflow; no log evidence). | `routes/cron.js:728-770`; roadmap doc |
| Concurrency control | **Defect, reproduced:** the mission advisory lock is session-level and is taken and released through `pool.query`, so the two calls can run on different pooled connections. The unlock then fails silently, the original connection keeps the lock, and the next session gets `tme_transaction_overlap`. The holding connection can still re-enter, because session advisory locks stack, so mutual exclusion is not guaranteed. Reproduced 3/3 on PostgreSQL 16 using the deployed module. | `TransactionalPersistence.js:89-155`; `acquisitionMissionPersistence.js:949-1005`; `acquisitionMissionRuntime.js:288` |
| Mission upsert | Last-writer-wins `ON CONFLICT DO UPDATE` with no version predicate. It relies entirely on the lock above. | `acquisitionMissionPersistence.js` `persistMission` |

### Gate D — Outcome verification: **PARTIAL**

| Capability | Finding | Evidence |
|---|---|---|
| Distinguishes the states | Yes, structurally: 13 states, only 3 reportable as done. `RoutingFallbackIntegrity` turns declined commands into "Not executed". | `ExecutionEvidence.js:33-56` |
| Independent verification | **No.** Classification reads the handler's returned `records` (`status`, `providerMessageId`). It does not re-read persisted `acquisition_outbound_items` or query the provider. | `ExecutionEvidence.js:114-130`; `ExecutionRouter.js:550` |
| Maximum level | `provider_accepted`. Delivery, open, reply and meeting are explicitly "not implied". There is no Max-side correlation of webhook outcomes to the execution record. | `ExecutionEvidence.js:66-74` |
| Unit evidence for the verified-send path | **Broken.** `spec071ExecuteOutbound` 7 fail (sent ≥ 1, provider calls, maxSends/replay, idempotency); `spec186CanonicalSender` 3 fail; `spec212ProspectMessageBinding` fails; `specExecutionApprovalHydration` 1 fail. All are fail-closed (nothing sends). | local run on `357e747` |
| Discovery verification | Requires a non-synthetic result and a discovery record. `spec175` 3 fail (blocking classification drift, R6). | local run |
| Production | **UNVERIFIED.** S5 and S8 have never been executed. Riley cannot read the inbox on Render, so reply outcomes are not collected. | §7 R-4 |

### Gate E — Recovery and escalation: **FAIL**

| Capability | Finding | Evidence |
|---|---|---|
| Atomic stage commit/rollback | Implemented (SPEC-131); `executing` never durable; presentation failure does not undo a commit. | `TransactionalExecution.js:395-425` |
| Fail-closed persistence | `MISSION_PERSISTENCE_UNAVAILABLE` instead of an in-memory fallback. | CLAUDE.md SPEC-MAX-RELIABILITY-004 §3 |
| Bounded retries / timeouts | **None** in the router or TME execution path (no timeout or deadline in `ExecutionRouter.js` / `TransactionalExecution.js`). A hung specialist call holds the HTTP request and the mission lock. | grep |
| Idempotency / duplicate external actions | Outbound has per-item provider receipts (governed path). But the replay-suppression unit tests fail (spec071), and `alreadyExecuted:true` is produced only by `AmoOperatorApproval.js` (`:229`, `:973`), not by the outbound stage. | local run; grep |
| Silent failures | **Confirmed in production:** cleanup job failing hourly; Paige 5/5 channels failing daily; Riley failing daily. All of them only write log lines; nothing raises an alert. Shadow observer failures are silent by design (prior P2, still open). | §7 |
| Escalation | No operator-facing escalation for scheduled-job failure. Max's briefing does not report job health. | — |
| Restart resilience | Missions survive (PostgreSQL). Workspace sessions do not survive by default. After a restart, a `sessionId` with no `context` returns HTTP 400 ("sessionId or context is required"). Six production deploys in about 48 h each reset every session. S4 (restart test) has never been executed. | `WorkspaceEngine.js:805-808`; Render deploy list |

### Gate F — Operational continuity: **PARTIAL**

| Capability | Finding | Evidence |
|---|---|---|
| Objectives, decisions, constraints | Durable and tenant-scoped. Authorization constraints are monotonic and durable. | persistence and AMO tests |
| Outstanding work / checkpoints | Only as mission stage plus pending decision. No durable record of in-flight or partial work (Gate C). | — |
| Conversational resumption | `recoverConversationSession` re-validates mission referents and never restores approvals. It requires `MAX_CONVERSATION_CONTEXT` (and `MAX_CONVERSATION_LEDGER` for the ledger), whose values are UNVERIFIED. Retention cleanup is failing (401). | `WorkspaceEngine.js:573-644`; Actions run 45457703652 |
| Change focus and resume | Within one process: yes (`operationalFocus`, ledger). Across restarts: only if the flags are on. Otherwise the operator must re-open and re-establish context from mission inspection. | code |

---

## 5. Supporting evidence (index)

| ID | Evidence |
|---|---|
| E1 | Render deploy list: live `dep-dc2p9obm8hqs7386utd0` @ `357e747`; prior deploys `0728ba2`, `f414087`, `13e7e0a`, `14f9166` (×2), `321b538`, `ecfb7b6`. |
| E2 | Render startup log 2026-11-24 13:46:11Z: `[startup] background workers disabled for Render API cutover`; Levi clock "started (independent of background workers)". |
| E3 | Render app logs 2026-11-23 12:00:14Z and 2026-11-24 12:00:14Z: `✗ Pulseforge/<channel>: Could not resolve authentication method. Expected either apiKey or authToken…`, "Paige complete — 0 posts drafted …, 5 channels failed". `paigeAgent.js:3,16` uses `utils/meteredAnthropic`. |
| E4 | Render app logs 2026-11-23 13:30:11Z and 2026-11-24 13:30:11Z: `[Riley] Gmail processing failed: Missing Gmail OAuth credentials`. |
| E5 | Actions `max-context-cleanup-hourly.yml`: 44 runs; the latest 5 inspected all `failure`. Run 45457703652 log: `curl: (22) … 401` ×3, `{"error":"Unauthorized"}`. Render `http_request_count` shows 16–20 × 401 per 6 h from 2026-11-23 06:00Z, consistent with 3 attempts per hour. |
| E6 | Render app log 2026-11-24 13:54:53Z: `[command-deck] spatial content recs failed: relation "content_recommendations" does not exist`. |
| E7 | `git log origin/main..origin/migration/render-current-main`: 18 commits, including Max-touching `e592578` (ExecutionRouter/TME pilot policy) and `f414087` (Max operational-read change). |
| E8 | Actions: PR CI `failure` on `f414087` (run 45209171488) and `0728ba2` (run 45275321229, `anchor-outbound: failure`). Both were deployed (`dep-dc262pe8bjmc739tq6pg`, `dep-dc2bfvs9v7es73bb1ij0`). |
| E9 | Actions `full-regression.yml`: latest runs on `main` @ `76a1059` only; push run 45076295804 `failure`, scheduled runs `success`. Never run on `357e747`. |
| E10 | Local: `npm run test:max:unit` on `357e747` → tests 2,926, pass 2,925, fail 0, todo 1. |
| E11 | Local, offline harness on `357e747`, acquisition-mission suites: spec135 1F, spec170 1F, spec071 7F, spec186 3F, spec139 3F, spec169 4F, adr075 2F, spec175 3F, spec157 1F, acquisitionMission 2F, specExecutionApprovalHydration 1F, spec212 1F (29 failing tests). `test:amo` is absent from `pr-ci-lean.yml` and `full-regression.yml`. |
| E12 | Local disposable PostgreSQL 16, deployed `TransactionalPersistence.js`: with the holder connection busy during release, `still granted after release: [<holder pid>]`, then `next request, other session -> tme_transaction_overlap`, 3/3 runs. |
| E13 | Render service list: `pgAdmin-dpg-dbtgtloae00c73dd8040-a` created 2026-11-24 00:00Z, `ipAllowList 0.0.0.0/0`, `autoDeploy: yes` from `github.com/render-examples/pgadmin` `main`. |
| E14 | Render Postgres `dpg-dbtgtloae00c73dd8040-a`: 10 GB disk, `diskAutoscalingEnabled: false`, `ipAllowList: []`, 1–3 active connections. |

---

## 6. Confirmed defects and architectural gaps

| # | Defect / gap | Type | Severity |
|---|---|---|---|
| D1 | Background workers disabled in production since the cutover (Anchor governed scheduler, warm routing, unenriched enrichment, Mira, governed-outbound execution clock for mailbox tenants) | Render regression | P0 |
| D2 | Hourly Max context cleanup returns 401 on every inspected run (CRON_SECRET on Render does not match the GitHub secret, or is unset) | Render regression | P1 |
| D3 | Anthropic credential not resolvable on Render (Paige). Max LLM paths are probably affected. | Render regression | P0 |
| D4 | Riley Gmail credentials absent: no inbound reply evidence | Render regression | P1 |
| D5 | Max decay (`max-decay-daily`) not replaced; no `[max_decay_cron]` log since the cutover | Render regression | P1 |
| D6 | Mission advisory-lock leak across pooled connections | Code (confirmed) | P0 |
| D7 | No durable delegation, timeout or abandonment model for Max-dispatched work | Architecture | P0 |
| D8 | Outbound verification relies on handler-reported records, with no independent readback. Its unit evidence is broken (R4). | Architecture + tests | P0 |
| D9 | `test:amo` absent from CI. Full regression not run on the deployed branch. Red-CI commits deployed. | Process | P0 |
| D10 | Routing defects R2, R3, R7, R9, R10 open | Code | P1 |
| D11 | No alerting for scheduled-job or observer failure | Observability | P1 |
| D12 | Sessions bound to tenant, not user; lost on every deploy unless the continuity flags are on | Design | P2 |
| D13 | `content_recommendations` relation missing in production | Schema | P2 |
| D14 | Docs (CLAUDE.md, acceptance procedure, release checklist) still describe Railway | Docs | P2 |

---

## 7. Render migration findings

### Confirmed regressions

| ID | Regression | Max impact |
|---|---|---|
| R-1 | `PF_DISABLE_BACKGROUND_WORKERS=1` in production (`server.js:109-139`; startup log E2). | Anchor poll/tick/control and Babrun's mailbox execution clock do not run, so Max inspects stale operational state and cannot observe replenishment. |
| R-2 | Context-cleanup cron: 401 every hour (E5). | Expired `max_conversation_context` rows are not purged. Indicates a cron-secret mismatch affecting every Bearer-authenticated route. |
| R-3 | Anthropic credential missing (E3). | Paige produces nothing. Max's presentation/reasoning model calls probably fail or degrade to deterministic output (inferred; no Max turn observed). |
| R-4 | Gmail credentials missing (E4). | Reply outcomes and "who replied" operational reads lack data. |
| R-5 | Max decay has no caller (no workflow; no log line). | Recommendation/attention decay is not applied. |
| R-6 | HTTP request logs unavailable on this Render plan/API. | Cron callers and Max traffic cannot be audited (Railway exposed them). |
| R-7 | Deploy-branch split: production = `migration/render-current-main` (manual), `main` behind; scheduled workflows run only on `main`. | CI and scheduled jobs do not test or describe what is deployed. |
| R-8 | `content_recommendations` missing (E6). | Command Deck spatial recommendations fail. |

### Suspected risks (not confirmed)

| ID | Risk | How to confirm (read-only) |
|---|---|---|
| S-1 | If `CRON_SECRET` is **unset** on Render, the 13 legacy handlers using `if (process.env.CRON_SECRET && secret !== …)` (e.g. `routes/cron.js:330`) accept unauthenticated triggers of agents. | Check presence of the variable name in the Render dashboard. Do not test by calling a cron. |
| S-2 | Database disk: 10 GB, no autoscale. The receiver kept pre-cutover copies (`pulseforge_pre_cutover_*`, the Nov 17 database) on the same instance. | Render dashboard disk usage, or `pg_database_size` via an approved read-only credential. |
| S-3 | pgAdmin is publicly reachable and auto-deploys from a third-party repository (E13). | Owner review. Consider suspending (needs approval). |
| S-4 | Single instance (`WEB_CONCURRENCY=1`). Every deploy drops in-memory sessions and in-flight requests. | Render events. |
| S-5 | Other Railway env names (about 150 listed during cutover) may be missing (pattern of R-3/R-4). | Name-by-name reconciliation by the owner. |
| S-6 | JEV-006 and 2026-11-09 migrations: applied state unknown on the restored database. | `scripts/checkDecisionShadowSchema.js` via an approved read-only credential. |
| S-7 | Whether cron-job.org's 15-minute `/cron/anchor-max-outbound-control` reaches Render. No app-log trace was found. | cron-job.org history (owner). |

Unaffected or healthy: deploys and builds succeed; `/healthz` health check configured; database CPU around 0.018 and 1–3 connections; startup schema phases succeed; the Levi weekly clock starts.

---

## 8. Prioritized remediation backlog

Severity: P0 blocks P0 exit; P1 required for exit; P2 can follow.

| ID | Sev | Item | Components | Evidence | Scope | Depends on | Acceptance criteria |
|---|---|---|---|---|---|---|---|
| M1 | P0 | **Render configuration reconciliation** (operations only) | Render env | R-2, R-3, R-4, S-1, S-5 | Owner compares the Railway variable-name inventory with Render; sets `CRON_SECRET` (matching GitHub), `ANTHROPIC_API_KEY`, Gmail credentials; records the decision on `PF_DISABLE_BACKGROUND_WORKERS` (stays `1` until M10 unless approved). No outbound flags change. | E3–E5 | — | Cleanup workflow green 3 consecutive hours; Paige assessment runs without the auth error (drafts only, approval-gated); Riley poll succeeds; a variable-name diff is recorded. |
| M2 | P0 | **Production truth surface** (read-only) | new authenticated admin route or CLI | R-6, S-6, prior P2 | Booleans and versions only: flag presence/values for Max gates, schema/migration presence (JEV-006, context, shadow, `content_recommendations`), last success per scheduled job, background-worker state, deployed SHA. No secrets, no row content, no DDL. | E2, E6 | — | Admin GET returns the report on Render; a test proves no write SQL and no secret values. |
| M3 | P0 | **Fail-closed cron auth** for all legacy handlers | `routes/cron.js` (13 sites) | S-1 | Replace `if (CRON_SECRET && …)` with the existing fail-closed timing-safe helper. | code | M1 (the secret must exist first) | Unit test: unset secret → 401 for every route; existing cron callers still succeed after M1. |
| M4 | P0 | **Advisory-lock correctness** | `TransactionalPersistence.js`, `acquisitionMissionPersistence.js`, `acquisitionMissionRuntime.js` | D6, E12 | Acquire and release on one dedicated client, or use `pg_advisory_xact_lock` inside the stage transaction; check the unlock result; add a version predicate to the mission upsert. | E12 | — | Disposable-PostgreSQL test reproducing E12 fails before and passes after; a concurrent stage commit on one mission yields exactly one commit. |
| M5 | P0 | **CI truth on the deployed branch** | workflows, `test:amo` fakes | D9, R5 | Add `test:amo` to the PR lane and full regression; fix in-memory SQL fakes (R5); run full regression on `migration/render-current-main` (or reconcile branches and deploy only from one); require green CI before a manual deploy. | E8, E9, E11 | — | `test:amo` green or each remaining failure explicitly quarantined by owner decision (no silent skips); full regression green on the deployed SHA. |
| M6 | P0 | **Verified outbound path** | `ExecutionEvidence.js`, `EmmettOutboundExecution.js`, `OutboundExecution.js` | D8, R4 | After commit, re-read persisted execution records by `transactionId` and classify from the database, not the handler result; produce `alreadyExecuted` on replay; correlate Brevo webhook events to the record (report as a separate "observed outcome" level). | E11 | M4, M5 | spec071/186/212/hydration green; a replay test shows no second provider call; classification reads from PostgreSQL in a disposable-PG test. |
| M7 | P1 | **Routing defects** R2, R3, R9 (+ R7/R10 triage) | `PendingDecisionResolver.js`, `Scout.discover`, legacy approval path | D10 | Narrow fixes only. Decide on enabling `MAX_GATE_BOUND_APPROVAL`. | E11 | M5 | spec135/spec170 green; R9 reproduction yields "Not executed". |
| M8 | P1 | **Stalled-work detection (read-only)** | Operational inspection, execution audit | D7 | Read-only detector over missions, pending decisions and execution audit: stale pending decisions, missions without progress for N days, stages with an audit gap. Surfaced in the Max briefing. No archival or mutation (roadmap boundary). | — | M2 | Fixture test per class; production run lists the tenant-10/13/21 inventory without writes. |
| M9 | P1 | **Silent-failure alerting** | scheduled jobs, observers | D11 | Persist the last success/failure per scheduled job and observer; alert (existing Telegram/email path) after N consecutive failures. | E3–E5 | M2 | Simulated 401 cleanup produces one alert; no raw text stored. |
| M10 | P1 | **Render-native acceptance run** S1–S9 + restart | acceptance procedure | §2 | Rewrite `P0-MAX-PRODUCTION-ACCEPTANCE.md` for Render (DB endpoint, manual deploy, same-SHA redeploy for S4); run with a dedicated acceptance tenant. Each authorization (A1–A6) is separate. | — | M1–M7 | All exit criteria in §10. |
| M11 | P1 | **Execution timeouts** | router / TME | Gate E | Per-intent deadline; on expiry classify `outcome_unknown` and hold, never retry blindly (consistent with the durable-scheduling doctrine). | — | M4 | Hung-handler test: request returns within the bound, mission lock released, state `outcome_unknown`. |
| M12 | P2 | Continuity enablement | context flags | D12, Gate F | After M1/M2: enable `MAX_CONVERSATION_CONTEXT` for an isolated tenant; bind sessions to user as well as tenant. | — | M1, M2, M10 S4 | Restart recovery test in production for the acceptance tenant. |
| M13 | P2 | Hygiene | pgAdmin, docs, schema | S-3, D13, D14 | Owner decision on pgAdmin; update CLAUDE.md and the release checklist for Render; decide on `content_recommendations`. | — | — | — |

---

## 9. Recommended production acceptance tests

All tests run on Render against a dedicated acceptance tenant, never Anchor (10), Babrun (13), MSHI (2) or Levi (21) data, unless a test is explicitly read-only.

1. **T-CTX (read-only).** Open the workspace as admin with tenant T active. Verify that `workspaceOwnership` and the operational read for T contain no foreign ids. Repeat with `client_id` override attempts → 403 or ignored.
2. **T-ROUTE.** Original S1, S2 and S7, plus R2 ("Begin discovery." must not approve a plan) and R9 (bare "approved" with no pending decision → "Not executed").
3. **T-AMEND.** Original S3.
4. **T-RESTART.** Original S4 via a same-SHA manual redeploy. Additionally: an in-flight `sessionId` after restart either recovers (flags on) or returns a clean re-open prompt, never a 500.
5. **T-DISCOVERY.** Original S5 with the Places cap enforced by `api_spend_policies`.
6. **T-DISPATCH.** Original S6. Each state comes from the database readback (M6).
7. **T-SEND.** Original S8 only under its hard single-recipient precondition, plus replay → `already_executed` with zero provider calls, plus a Brevo webhook correlated to the execution record.
8. **T-SYNTH.** Original S9.
9. **T-CONCURRENCY.** Two simultaneous approval requests on one mission → exactly one commit, the other `tme_transaction_overlap`, no leaked lock (check `pg_locks` via M2).
10. **T-JOBS.** Every scheduled job (cleanup, decay, Paige, Riley, Emmett, Levi weekly) shows last success under its SLA in M2. A forced failure produces an alert (M9).
11. **T-TENANT21 (read-only).** The post-deploy step-4 checks from `docs/audits/RELEASE-READINESS-2026-11-24.md`: admin `POST /api/run/scout?client_id=21` → 403 `tenant_pilot_policy_denied` (audited, no work), and the pre-invite SQL baseline.

---

## 10. Proposed P0 exit criteria and observation requirements

**Exit criteria (all required; human decision to close):**
1. M1–M7 complete; M8–M11 complete or explicitly deferred by executive decision with a recorded rationale.
2. T-CTX through T-CONCURRENCY pass on the deployed SHA. T-SEND passes **or** is recorded as blocked by its precondition, with an explicit closure decision on outbound evidence.
3. CI: `test:max`, `test:amo` and full regression green on the exact deployed SHA. No deploy from a red commit during the window.
4. No unauthorized external effect, no cross-tenant read, no spend beyond the approved cap.
5. Every production fact in the closure report comes from M2, Render logs or production SQL. None is inferred from code.

**Observation window:** 7 consecutive calendar days on Render, including at least 5 business days, after the last P0 code deploy:
- 0 unalerted scheduled-job failures; every alert resolved or explained;
- 0 `tme_transaction_overlap` without a concurrent request (lock-leak regression signal);
- 0 Max responses reporting completion where `execution.state` is not in the verified set (sampled from the execution audit);
- at least 1 restart (planned or natural) with mission state unchanged;
- daily M2 snapshot archived.

---

## 11. Scout boundary and tenant validation readiness

**Scout boundary.** Max reads Scout state through `OperationalInspection` and the EvidenceRegistry, both SELECT-only through `readOnlyQueryable`. It dispatches Scout only through `routeExecutionRequest`, which is subject to authorization constraints, the research pause and grants, and the tenant-21 pilot policy. In production, Max **cannot currently verify Scout operations**:
- background schedulers are off;
- Places research for tenant 10 is paused or grant-bound;
- request logs are unavailable.

Max inspection is safe to run (read-only), but its reports would describe a static state. No change to Scout is recommended in this P0.

| Tenant | Controlled-validation readiness |
|---|---|
| Anchor Cleaning (10) | **Read-only validation only.** Outbound was halted (`authorization_expired`, 2026-11-09 Railway logs). Governed scheduler off on Render. Research grant state UNVERIFIED. Do not use for S5/S8. |
| Babrun (13) | **Read-only validation only.** Its mailbox execution clock is disabled with background workers. Current outbound and research state UNVERIFIED. |
| Levi / LA Endeavors (21) | **Not yet verified as validation-ready.** The pilot policy is in the live commit (`357e747` contains `e592578`). The release-readiness doc makes credential issuance conditional on step 0 (pre-invite SQL) and step 4 (post-deploy checks). No evidence either ran, and the expected startup `replay_blocked` catch-up line is absent from the 13:46Z startup log. **Levi's login is UNVERIFIED.** Credential delivery does not establish that he signed in. Max is locked for a `client` user until the Blueprint is approved and the AIM published. The Blueprint has no approved financing range, so Max will probably present the onboarding lock. Use tenant 21 for T-TENANT21 and read-only operational reads only. |

---

## 12. Smallest coherent engineering sequence

1. **M1 + M2** (operations reconcile and the read-only truth surface). Nothing else can be verified without these.
2. **M3 + M4** (two small, isolated safety fixes, each with a failing-first test).
3. **M5** (CI truth on the deployed branch; single deploy source; red CI blocks deploy).
4. **M6 + M7** (verified outbound readback and the open routing defects).
5. **M9**, then **M8** and **M11** (alerting, read-only stalled-work detection, execution deadlines).
6. **M10** (Render-native acceptance run including restart and concurrency).
7. Observation window (§10). Then the executive closure decision.

Explicitly out of scope until P0 closes: semantic routing promotion, lifecycle archival, durable stage scheduling rollout, background-worker re-enablement beyond what M10 approves, and any new Max capability.

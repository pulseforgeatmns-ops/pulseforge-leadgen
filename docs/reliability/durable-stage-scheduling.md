# Durable governed stage scheduling: proposed rollout

LOCAL IMPLEMENTATION ONLY. No migration, deployment, flag change, or live proof has been performed. This remains part of the P0 engineering freeze.

## Why two proposed tables

Existing acquisition_outbound_items/provider boundary receipts already protect each sending item. Existing control advisory locks serialize a running control loop. Neither stores the due time and restart-safe lifecycle shared by the timer and external cron. Reusing item locks as scheduler state would conflate business work with periodic observation and introduce conflicting ownership.

Proposed migration `2026-10-21-governed-stage-scheduling.sql` adds:
- governed_outbound_stage_schedule: one row per allowed tenant/stage, due time, run, monotonic fence, claim token/lease, start marker, attempt count, outcome/freshness.
- governed_outbound_stage_runs: one logical run per tenant/stage/due time; attempt/fence/status, timestamps and bounded canonical evidence references. No email bodies, credentials or provider payloads.

No existing rows/tables are altered; no automatic schema initialization or migration occurs at startup. Missing schema fails closed in durable mode. Migration uses transaction, two-second lock timeout, thirty-second statement timeout. No migration should be applied until reviewed by Jacob.

## Ownership and safety

Existing timer/cron entry points call one coordinator when GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED=true. Default remains false for compatibility, NOT authorization to enable legacy sending. No independent extra timer is introduced. Cron replies say requested, not admitted or completed. Actual execution outcome is in the durable run record.

Short row-lock transactions claim/start/finalize work. No scheduler connection is held while a handler runs. An atomic start requires the current token, fence and unexpired database-time lease. Terminal writes and heartbeats enforce the same identity. Business handlers retain existing send/control locks, grant checks, approval binding, inbox freshness, research pauses and provider uncertainty rules.

Per-process, per-pool admission deduplicates pending tenant/stage requests and permits at most two handlers, with only one active stage per tenant. Pools smaller than four fail admission. A global supervisor separately caps child count across pools. Child business pools are isolated from the parent pool, each bounded to four connections. Rollout must measure nested handler demand, parent users and aggregate replica connection limits. An exhausted business pool is a deployment blocker, not justification to grow connections blindly. Pending work stays due; queued acknowledgements are not execution results.

A poll completion requires actual same-tenant, non-skipped successful integration receipts. Empty mailbox is valid only after a real successful poll. Missing credentials, overlap, not-pollable, error or arbitrary empty object does not refresh successful poll time. Tick requires the same tenant's last verified poll to be fresh and not running/uncertain. Existing downstream live inbox gate remains authoritative. Policy holds schedule a future observation, never count as successful sending.

## Crash windows and retries

- Before claim: no work is owned and nothing executes.
- Claim committed, start not committed: expired claim can be recovered with a new fence and same logical run; old worker's start fails. At most three admission attempts, exponential bounded backoff; exhaustion holds for review.
- Start commit acknowledgement lost: handler is NOT invoked. Retry is reported only if a safe retry receipt was persisted; otherwise result is uncertain. A persisted start causes eventual quarantine.
- Handler invoked, provider outcome or terminal write unknown: quarantine. No generic thrown error is considered proof that no side effect occurred.
- Lease expires while old handler runs: next observation marks it uncertain and does NOT issue a replacement. Late heartbeat/start/finalize fails the fence/status/lease check. Existing per-item provider evidence remains the only authority on whether an email was accepted.
- Database connection loss: no success is inferred; no blind resend occurs. Claim expiration/quarantine records the gap after database recovery.
- Hung execution: poll/tick/control maximum runtime defaults 2/5/20 minutes. Heartbeat marks the stage uncertain when the bound is exceeded, stops renewing, and never releases/replays the underlying operation via Promise.race. If the event loop itself is blocked, database lease expiry provides the same quarantine on another worker.

Quarantine is safe containment, NOT automated completion/recovery of unknown external work. Supervised worker termination and evidence reconciliation remain required before releasing an uncertain stage. No automatic uncertain-release API is included.

## Required reconciliation procedure (approval required before live action)

1. Keep the affected stage held. Identify exact tenant, stage, run ID, fence, timestamps and canonical references. Never erase rows or reset attempt counts to make the dashboard green.
2. Stop/drain the relevant worker or prove the old invocation has terminated. A lease expiry alone is not proof the external operation stopped.
3. Read canonical item/execution/provider records and reconcile every attempted/uncertain provider call using the existing evidence-based reconciliation path. For control/poll, account for downstream provider/candidate/classification work too.
4. Distinguish verified completion, verified no side effect, and still unknown. Unknown stays held. Any recovery mutation must be approved with the specific tenant/run and cited evidence, then performed transactionally under the current fence. No generic blind reset command is provided.
5. After approved recovery, verify the durable record and run one bounded stage with existing gates. Reconcile both scheduler and canonical outcomes before restoring recurring execution.

## Deployment plan for review

1. Pass independent code review, final combined CI on deployed Node version, and real disposable PostgreSQL contention/crash tests. Unit mocks alone do not validate SQL atomicity.
2. Restore authorized production read access. Verify exact deployed SHA, timers/cron/CLI owners, flags, all tenants' existing holds, pool capacity and canonical migration readiness. Do not infer these from source.
3. Quiesce all automatic invocations under separately approved operational controls. Verify no running/uncertain provider work can be replayed. Mixed old/new runtimes are not allowed.
4. Apply the additive migration explicitly; inspect resulting schema. Deploy reviewed code with durable mode OFF and existing safety holds unchanged.
5. Enable durable ownership uniformly only after old workers/cron entry points are quiescent. Start with a bounded, approved tenant/stage canary. Verify requested→claimed→running→terminal plus canonical evidence; zero-work canary proves storage/ownership only.
6. Exercise real end-to-end qualified prospect→approval→claim→provider acceptance→canonical readback under explicit existing sending authorization. Delivery is a separate measured outcome.
7. Exercise controlled failure/restart/recovery without duplicate sends, tenant crossover, lost due work or false success. Then sustained observation: recommend five consecutive business days, with >=5 qualified new first touches daily when supply/safety permit. Window remains a proposal, not a claim of achieved acceptance.
8. Max health reporting must read stage status/freshness plus canonical business records; stage completed never means revenue, delivered email or fulfilled delegation.

## Rollback and shutdown

A flag flip alone is NOT a safe rollback. The timer stops admitting when the flag is disabled, but external cron would select legacy mode, so cron must first be quiesced. Stop automatic triggers, call stop/stopAndDrain, and inspect active/uncertain runs. Stop/drain does not forcibly cancel work. If a handler is hung, supervise termination and preserve quarantine; do not immediately restart legacy execution.

Retain both new tables and all evidence. Roll code/flag back only after proving no durable invocation can still execute and reconciling every uncertain outcome. No DROP TABLE or evidence deletion is part of rollback. Existing per-item uncertainty holds remain in force.

## Current validation limits

Hermetic coordinator tests exercise receipt truth, no unsafe retry, failure isolation, bounded admission and runtime quarantine. PostgreSQL test is present and mandatory but local initdb/pg_ctl are unavailable; it fails rather than skips. Live database, provider, browser and sustained-production proof have not run. This branch is NOT production-ready until those checks and rollout approval are complete.

## Historical in-process blocker: simultaneous hung handlers

The bounded pool admission is safe containment, but two permanently hung handlers occupy both slots and block healthy tenants queued behind them. A diagnostic characterization test reproduces this; it is NOT a passed tenant-independence acceptance test. Runtime quarantine intentionally does not release capacity while underlying operations may still hold connections or perform external work. This in-process path must NOT be enabled in production; the approved local supervised integration below replaces it for default execution.

The supervised implementation below provides local-process termination and modeled uncertain-outcome containment. Cooperative cancellation at all I/O boundaries, real PostgreSQL/provider cleanup and canonical reconciliation have NOT been proved. Simply increasing the pool or releasing slots while the worker still runs is not a fix. No sustained reliability or complete external failure-recovery claim is made.

## Approved local supervised integration update

The user approved local integration of the isolated-process design. The runtime default handler now invokes `governedOutboundStageSupervisor`, not an in-process business handler. The supervisor limits active children to two across pools and one per tenant; coordinator queues at most the fixed tenant/stage key set. Each child reuses the existing db singleton with max four connections, 5-second connection timeout and 10-second query timeout. Thus child pools add at most eight connections per replica, in addition to the existing parent/web pool (normally ten). Measure total replica capacity and nested handler demand before any deployment. There is no new remote infrastructure or credential generation.

Linux and Python 3 are mandatory runtime prerequisites. The checked-in standard-library launcher sets PR_SET_PDEATHSIG to SIGKILL before verifying the expected parent PID, then execs the Node worker. Unsupported platform, missing Python or prctl failure fails closed. Local tests prove actual parent SIGKILL kills its hanging child; production image availability remains unverified and must be checked before rollout. Workers also exit on IPC disconnect and use a five-second parent-heartbeat watchdog. Neither mechanism claims to cancel provider-side requests.

Default runStage first durably starts the claim, then launches its bound child. Parent validates tenant/stage/run/fence/instance on terminal IPC, rejects duplicate/invalid messages, validates positive stage-specific proof, and persists only whitelisted canonical references. Child IPC is not durable success. Graceful stop followed by a fixed, non-extendable forced-termination deadline ensures malformed IPC cannot extend worker lifetime. Capacity is returned only after confirmed child exit or confirmed spawn failure.

The supervisor attempts durable uncertainty immediately on failure, independently of terminating the child. Quarantine writes are idempotent for the exact run/fence and cannot overwrite newer work. If persistence is unavailable or its acknowledgement times out, the result explicitly remains quarantine-unverified. After confirmed child exit, healthy tenant capacity may be reused: the already committed started claim remains fenced, unexpired acquisition remains blocked, and expired-started acquisition quarantines rather than retries. This is NOT a successful quarantine acknowledgement or permission to replay the affected stage. Parent storage acquisition/query/operation deadlines destroy late or poisoned clients; bounded acknowledgement waits never turn an unknown outcome into retry-safe success.

`stopAndDrain` closes admission before clearing unclaimed queued requests, terminates active children and waits for their exit and stage settlement. Restart is required to reopen admission. `stop` alone stops only the timer. Quiesce external cron before either rollback or shutdown; preserve uncertain claims and reconcile canonical/provider evidence before legacy execution.

The earlier two-hang characterization remains a test of injected in-process handlers, not the production default path. New actual-supervisor tests exercise two hung children, healthy capacity recovery, hung quarantine persistence, parent death, malformed IPC flood, stale/duplicate receipts, stage evidence validation, and shutdown. These are hermetic execution proofs with inert children, not real provider or PostgreSQL acceptance. Production remains blocked by real PostgreSQL contention/crash proof, runtime prerequisite/capacity verification, integrated handler E2E, independent final review and sustained production observation.

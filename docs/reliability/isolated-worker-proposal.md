# Proposed isolated execution boundary (requires architecture review)

Status: user approved local integration after this proposal. Actual local scheduler wiring now uses supervised workers; see durable-stage-scheduling.md for the implemented limits and evidence. Production topology, schema, credentials and deployments remain untouched. This document retains the design rationale and remaining rollout gates.

## Evidence and choice

The existing poll, tick and control call graphs do not expose an end-to-end cooperative cancellation contract. `governedOutbound.js` awaits approval, preparation, adapter dispatch and canonical database recording; `tenantMailboxPollExecutor` and reply classification await provider and persistence operations; control admission owns an additional session lock until its handler settles. Scattered SQL timeouts do not establish cancellation across these boundaries. Adding an AbortSignal to the outer promise would not prove inner operations stopped.

Recommend a supervised one-stage child process, rather than unsafe Promise.race release or assuming a transport abort reverses a provider request. This changes process topology; local integration approval was received. Deployment remains separately unapproved. The prototype alone demonstrates feasibility only; actual integrated hermetic tests are documented separately.

## Exact integration changes proposed

1. Add a narrowly scoped stage worker entry point, not server startup. It imports the current stage handler and all existing approval/grant/inbox/provider guards unchanged. One process runs one tenant/stage/run and exits. Never load web timers or cron registration in the worker.
2. Parent acquires and starts the durable claim before handing over execution. Bind IPC messages to tenant, stage, run ID and fence, with a unique process instance. Parent retains the claim token; child does not renew, finish or allocate alternate claims. Child reports only bounded status and canonical evidence references, not credentials, provider bodies or prospect data.
3. Parent retains heartbeat, durable terminal persistence and quarantine. Child completion IPC is NOT business success until positive stage evidence and the canonical references have been persisted under the active fence. Receipt/commit acknowledgement loss stays uncertain.
4. Deadline, child crash, invalid IPC, disconnect or parent shutdown triggers uncertainty. Request graceful child shutdown, then force termination after a reviewed bounded grace period. Only a confirmed process exit permits releasing that child's local execution capacity. Failure to confirm exit retains the slot and raises an operational fault. Do not reuse a PID alone as proof of identity across restart.
5. Parent restart may see an orphan worker. Durable claim expiry quarantines started work; no replacement same-stage claim is admitted. Process deployment supervision must terminate the complete old process tree and confirm exit before rollback or reconciliation. New admission must not silently bypass old claim state.
6. Preserve per-item/provider idempotency and uncertainty protection. Killing a local process proves only local worker exit. An already accepted request may still execute at the provider. The affected stage remains quarantined until canonical/provider reconciliation demonstrates completed or verified-not-performed outcome. Healthy unrelated tenants can run after bounded local resource cleanup.

## Resource and isolation budget

Implemented local budget supersedes the initial estimate: at most two child processes per app replica, one stage per child, each child database pool max four. The parent reuses its existing shared pool, normally max ten. Thus children add at most eight connections per replica, with a default combined ceiling of eighteen before other pools/replicas. This is a configured ceiling, not proof four connections meet every nested handler requirement. Measure nested lock/transaction demand with real disposable PostgreSQL and enforce query/connection timeouts that destroy uncertain clients safely. Add capacity for existing web/legacy pools explicitly; total replicas times pool ceilings must fit the reviewed database limit. Do not just increase existing pool size.

Use a per-tenant FIFO admission policy with at most one active child per tenant, so one tenant cannot monopolize both process slots. Each deadline recovers local capacity only after child exit. Cap pending keys by allowed tenant/stage pairs rather than unbounded queue size. Tenant identity is validated at admission and every IPC boundary; existing SQL and canonical tenant checks remain authoritative.

The prototype deliberately starts fixture children with only PATH and test inputs. Production credential access must reuse existing authorized runtime configuration without exporting secrets through IPC, files, logs or reports, and without generating new credentials/grants. Review environment allowlisting and least-privilege feasibility before wiring. This proposal does not authorize credential creation or persistent access changes.

## Failure and shutdown acceptance

Required integrated tests: two children hang while a healthy tenant queues; external attempt then hang; receipt before parent crash; wrong tenant/run/fence IPC; child crash/disconnect; parent connection loss before and after start commit; lease expiry; parent death with orphan child; database pool saturation; graceful shutdown and forced process-tree termination. Assert no duplicate provider attempts, no stale completion, bounded capacity recovery, no cross-tenant reads/writes, and uncertainty retained when provider outcome remains unknown.

Stop all admissions and timer/cron triggers first. Drain finished children, quarantine and terminate hung children, confirm process exit, reconcile every uncertain external outcome. A flag flip or code rollback alone is insufficient. Retain stage history/schema and canonical receipts. Returning to legacy execution requires separately reviewed operational approval and proof that no old worker can continue.

## Local proof versus missing proof

`prototypes/isolatedStageSupervisor.js` is not imported by runtime code. Its tests use inert child fixtures with no database, credentials or network. They demonstrate two hung workers terminated before healthy work progresses, failed quarantine callback does not pretend durable success, and crash/disconnect/wrong-run results remain uncertain. The prototype intentionally does not implement complete production shutdown/orphan supervision, durable reconciliation, dedicated DB pools, canonical receipt validation or real handler execution. These are required integration work after approval, not achieved acceptance.

Real disposable PostgreSQL, provider-boundary E2E and sustained production observation remain unperformed. No production reliability or complete failure recovery is claimed.

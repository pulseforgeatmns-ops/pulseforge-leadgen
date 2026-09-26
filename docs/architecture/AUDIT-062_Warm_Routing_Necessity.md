# AUDIT-062 — Warm Routing Necessity

| Field | Value |
|---|---|
| **Status** | Completed — read-only |
| **Date** | 2026-08-26 |
| **Related** | [AUDIT-058](AUDIT-058_Scout_Cognitive_Unification_Audit.md), [EPIC-001](../epics/EPIC-001_Scout_Cognitive_Unification.md), SPEC-171 / ADR-090 |
| **Scope** | Determine whether Warm Routing (`warmRoutingAgent.js`) performs any unique responsibility not already performed by the canonical cognitive architecture. Stop at the first architectural divergence. |
| **Observation** | `scripts/audit062WarmRoutingObservation.js` |

## Verdict

**Warm Routing does not sit on the canonical cognitive path.** It is a parallel operator-notification worker for already-known prospects.

- It is **not** invoked by chat, AMO, Mission Engine, or Scout.
- It owns **no** Market Definition, hypothesis, investigation, provider, identity, understanding, or judgment step.
- On a production AMO discovery mission it was **never loaded**.
- It makes **zero** LLM / token / discovery-provider calls.
- Default production posture is already off (`WARM_ROUTING_ENABLED` must be the string `true`).
- **First unique responsibility it still owns:** rising-edge operator incident routing (Telegram ping → Todoist queue → Mira capture pair). That work is outside the cognitive pipeline. Stop there.

---

## 1. Invocation

Warm Routing is **not** a mission specialist. It is an in-process worker plus an optional cron agent.

### Production entry points

| # | Entry point | Caller | Runtime | Mission types | Frequency |
|---|---|---|---|---|---|
| 1 | `startWarmRoutingScheduler()` `server.js:76` | Express boot (`node server.js`) | In-process `setInterval` | **None** | Every 10 minutes **only if** `WARM_ROUTING_ENABLED=true`. Otherwise logs and returns `null` (`warmRoutingAgent.js:1271–1274`). |
| 2 | `POST/GET /cron/warm_routing` `routes/cron.js:55,447–468` | Railway cron (`CRON_SECRET`) | HTTP → `mod.run()` | **None** | Schedule-dependent. Gated by `clients.enabled_agents`. Default client arrays **do not** include `warm_routing` (`utils/clientContext.js:218–316`). |
| 3 | `POST /webhooks/telegram/mira` callback `routes/webhooks.js:343–348` | Telegram `callback_query` matching `working\|today\|tomorrow:<uuid>` | HTTP webhook | **None** | On button click. Calls `handleWarmTelegramCallback` **even when the scheduler is disabled**. |
| 4 | CLI `node warmRoutingAgent.js` `warmRoutingAgent.js:1369–1375` | Operator / one-shot | Process | **None** | Manual |

### Paths that do **not** invoke Warm Routing

| Surface | Evidence |
|---|---|
| Dashboard `POST /api/run/:agent` | `warm_routing` is absent from `agentModules` (`routes/api.js:2898–2906`). Returns `Unknown agent`. |
| Chat / `/api/v1/amo/ask` | `submitChatExecutionRequest` → `routeExecutionRequest` (`packages/max/workspace/AcquisitionMissionExecution.js:154–188`). Specialist for discovery is Scout. |
| AMO execute `POST /api/v1/amo/missions/:id/execute` | `executeCanonical` (`services/acquisitionMission.js:98`) → `routeExecutionRequest`. |
| Mission Engine | No `require` of `warmRoutingAgent` under `packages/mission-engine`. |
| Scout / DiscoveryPipeline | No `require` of `warmRoutingAgent` under `packages/scout`. |
| Canonical Execution Router handlers | `defaultHandlers()` maps intents to AMO approval / autonomous progression (`packages/acquisition-mission/ExecutionRouter.js:94–141`). No Warm Routing handler. |

### Questions

| Question | Answer |
|---|---|
| Does every mission execute Warm Routing? | **No. Zero missions execute it.** |
| Only chat? | No. |
| Only acquisition? | No. |
| Only legacy? | It is a legacy **operational** Mira/Telegram worker (July 2026 edge-trigger), not a legacy cognition orchestrator. |
| Only Mission Engine? | No. |
| Only AMO? | No. |

**Runtime:** outreach CRM worker. Not cognitive runtime.

---

## 2. Call graph

### Canonical production discovery (observed)

```text
Operator (chat button / REST / voice)
  ↓  packages/max/workspace/AcquisitionMissionExecution.js:154  submitChatExecutionRequest
  ↓  packages/acquisition-mission/ExecutionRouter.js:350         routeExecutionRequest
  ↓  ExecutionRouter.js:267                                      dispatch
  ↓  ExecutionRouter.js:100                                      APPROVE_DISCOVERY → advanceDiscoveryAfterApproval
  ↓  packages/max/workspace/AmoOperatorApproval.js:782           advanceDiscoveryAfterApproval
  ↓  AmoOperatorApproval.js:513 / :531                           runScoutForAmoMission → Scout.discover
  ↓  packages/scout/Discovery.js:212                             discover
  ↓  packages/scout/DiscoveryPipeline.js:120                     runDiscoveryPipeline
  ↓  Stage 1  intelligence/MarketDefinition.js                   understand_market
  ↓  HypothesisInvestigationPlanner / CanonicalHypothesisEngine
  ↓  DiscoveryCapabilityGate
  ↓  runScoutAcquisitionIntelligence                             evidence collection
  ↓  Identity / Understanding / Judgment / Recommendation
```

Warm Routing does **not** appear on this graph.

### Warm Routing worker (only when enabled **and** seeded)

```text
server.js:76 startWarmRoutingScheduler
  ↓  warmRoutingAgent.js:1271 startWarmRoutingScheduler
  ↓  setInterval 10 min
  ↓  warmRoutingAgent.js:1102 run
  ↓  isWarmRoutingEnabled / isWarmRoutingSeeded          env + warm_routing_control
  ↓  withWorkerLock                                      pg_try_advisory_lock
  ↓  getWarmProspects                                    prospects + email_events
  ↓  resolveVerticalTier                                 utils/verticalTiers.js:31  warm_eligible
  ↓  buildCurrentEdgeEvents                              ICP_ELIGIBILITY / ENGAGEMENT_CLUSTER rising edge
  ↓  getPendingIcpScoreChanges + classifyIcpScoreChange  icp_score_history
  ↓  getPendingReplyEvents                               email_events + touchpoints
  ↓  processProspectEvents                               claim → coalesce → fire
       ↓  sendWarmTelegramMessage                        Telegram sendMessage
       ↓  recordFire                                     warm_trigger_fires + capture_inbox
  ↓  autoEscalateStaleFires                              Todoist tasks after 24h
  ↓  sendOverflowDigest                                  Telegram digest at 20:00 ET
```

No Scout. No ExecutionRouter. No Market Definition.

### Observed AMO mission (`scripts/audit062WarmRoutingObservation.js`)

| Field | Value |
|---|---|
| Mission id | `mission_bdb32c03-107e-46d2-80c8-ff3df9db4e38` |
| Router specialist | `scout` |
| Router action | `discovery_approved` |
| Runtime owner | `amo` |
| Scout outcome | `DISCOVERY_BLOCKED` (no external providers in this environment) |
| Scout stages executed | `understand_market` |
| Market Definition present | **yes** |
| `warmRoutingAgent` loaded during mission | **[] (never)** |
| Router latency | 52 ms |
| Scout.discover latency | 1 ms |

---

## 3. Inputs

Nothing inferred. Only values `run()` / `handleWarmTelegramCallback` actually read.

### `run(params)`

| Input | Source | Used for |
|---|---|---|
| `params.client_id` / `ACTIVE_CLIENT_ID` | `getRuntimeClientId(params)` `warmRoutingAgent.js:1103` | Tenant scope |
| `process.env.WARM_ROUTING_ENABLED` | `isWarmRoutingEnabled` `:237–238` | Hard gate |
| `warm_routing_control.seed_version` | `isWarmRoutingSeeded` `:241–256` | Seed gate (`2026-07-04-edge-v1`) |
| Prospect rows (id, email, phone, vertical, icp_score, status, DNC, mira_archived, last touch, company name) | `getWarmProspects` SQL `:258–401` | Candidate set (limit 500) |
| `clients.vertical_tiers` | joined in that SQL | `resolveVerticalTier` / `warm_eligible` |
| Human email opens/clicks in 24h | `email_events` via `OPEN_SOURCE.HUMAN` | `ENGAGEMENT_CLUSTER` |
| `warm_signal_state` | `:468–475` | Rising-edge vs already-active |
| `icp_score_history` | `:583–597` | `ICP_JUMP_15` / `ICP_CROSS_90` / `ICP_CROSS_80_RECENT` |
| Reply rows | `email_events` + `touchpoints` `:600–643` | `REPLY_RECEIVED` |
| Open `warm_trigger_fires` | `:882–892` | Coalesce evidence onto one incident |
| `warm_trigger_fires` ping counts | `:646–654` | Daily Telegram cap of 10 |
| `process.env.MIRA_TELEGRAM_BOT_TOKEN` | `:72–76` | Telegram send |
| `process.env.JACOB_TELEGRAM_CHAT_ID` | `:72–76` | Telegram destination |
| `process.env.TODOIST_API_TOKEN` | `:68–69` | Auto-escalation / button-queue tasks |
| Hardcoded Todoist project/section ids | `:16–17` | Task placement |
| `process.env.DASHBOARD_URL` / `APP_URL` | `:79–81` | Overflow digest link |

### Explicitly **not** consumed

Mission object, workspace, conversation, tenant AIM, memory graph, provider registry, Market Definition, investigation planner, previous Scout execution, ExecutionRequest, operator cognition intent.

### `handleWarmTelegramCallback(callbackQuery)`

Telegram `callback_query` (`data`, `id`, `message.chat.id`, `message.message_id`, `message.text`) plus the latest unresolved `warm_trigger_fires` row for that prospect (`:1307–1315`).

---

## 4. Outputs

### Return value of `run()`

| Field | Can be null / absent? | Used? | Consumer | When |
|---|---|---|---|---|
| `disabled` | yes (only on gate) | No (discarded) | Cron `mod.run().catch` (`routes/cron.js`); scheduler `.catch` | After HTTP 200 already sent |
| `reason` | yes | No | same | same |
| `fires` | 0 on gate | No | same | same |
| `scanned` | yes | No | CLI `JSON.stringify` only if `require.main` | Manual |
| `skipped` | yes | Internally mapped into observability | `reportWarmRoutingRun` `:1262–1267` | End of successful worker |
| `auto_escalated` | yes | No downstream cognition | `agent_log` payload via `logAgent('warm_routing_run')` `:1241` | End of run |
| `digest` | yes | No downstream cognition | `agent_log` | End of run |
| `results` | yes | Error sample only | `reportWarmRoutingRun` if a row has `error` | End of run |
| `attempts` / `successes` / `errorSample` | synthesized | Yes | `utils/agentObservability.reportAgentRun` → `agent_run_health` | End of run |
| `idle` / `skipped: true` (lock) | yes | Yes | observability as idle (attempts=0) `:1256–1260` | Concurrent worker |

**No canonical component reads this object.** Cron responds `{ success: true }` **before** `run()` finishes (`routes/cron.js:82–86`).

### Side-effect writes (the real outputs)

| Output | Used? | Consumer | When | Can be null? |
|---|---|---|---|---|
| Telegram `sendMessage` + inline keyboard | Yes | Jacob (human) | New incident and daily cap not exceeded | Yes — skipped if env missing or cap hit (`ping_sent=false`) |
| Telegram `editMessageText` | Yes | Jacob | Button resolve / 24h auto-escalate | Yes if no message ids |
| Todoist `POST /tasks` | Yes | Jacob's Todoist | `today` / `tomorrow` buttons; auto-escalate | Throws if token missing on that path |
| `warm_trigger_fires` row | Yes | callback handler, auto-escalate, digest, seed script | Each new incident | No on fire path |
| `warm_signal_events` pending→consumed | Yes | this agent only (idempotency) | claim/consume | No |
| `warm_signal_state` upsert | Yes | this agent only (edge memory) | every scanned prospect | No |
| `capture_inbox` `warm_signal` / `warm_signal_resolved` | Yes | `utils/miraContext.js:270–273` count; `utils/dailyHealth.js:96–105` count | fire + resolve | Capture id may be null if insert fails |
| `mira_warm_capture_log` | Written | Mira archive schema; no other reader in `packages/` | fire + resolve | — |
| `agent_log` (`warm_routing`) | Observability | dashboard activity if queried | each action | — |
| `agent_run_health` | Observability | stranded-agent alerts | end of run | — |

`/dashboard/warm` (`server.js:685`) serves the **same** `dashboard.html` as `/dashboard`. It is not a distinct Warm Routing UI.

---

## 5. Responsibility mapping

Warm Routing **does not implement** any of the canonical cognitive responsibilities.

| Responsibility | Canonical owner | Warm Routing? | Duplicate? |
|---|---|---|---|
| Market Definition | `packages/scout/intelligence/MarketDefinition.js` `buildMarketDefinition` | Does not read or write a Market Definition | **No** — not attempted |
| Segment resolution | `MarketDefinition.js:18–24` `SEGMENT_RESOLUTION_SOURCES` / `resolveSegmentKey` | Uses `prospects.vertical` + `clients.vertical_tiers` only | **No** |
| Hypothesis generation | `packages/scout/hypothesis/CanonicalHypothesisEngine.js` | No hypotheses | **No** |
| Provider selection | SPEC-182 registries / `DiscoveryCapabilityGate.js` | No providers | **No** |
| Runtime ownership | `packages/acquisition-mission/MissionRuntimeOwnership.js` via ExecutionRouter `:182` | No mission runtime | **No** |
| Capability discovery | `packages/scout/coverage/DiscoveryCapabilityGate.js` | No | **No** |
| Investigation planning | `HypothesisInvestigationPlanner` | No | **No** |
| Mission planning | AMO `advancePlanAfterApproval` | No | **No** |
| Identity | Identity Engine / Scout identity stage | Identifies prospects by existing `prospects.id` | **No** — lookup, not identity resolution |
| Understanding | Scout DiscoveryPipeline | No | **No** |
| Judgment | Scout | Threshold checks (ICP≥80, ≥3 opens, any click) are **rules**, not Scout judgment | **No** |

### Adjacent operational overlap (not canonical cognition)

| Signal | Warm Routing | Other owner | Same output? |
|---|---|---|---|
| Opens / clicks | Rising-edge `ENGAGEMENT_CLUSTER` → Telegram | `warmSignalAgent.js` writes `🔥 2ND OPEN` to Google Sheet; Riley `depositWarmSignalAction` (`rileyAgent.js:1338`) deposits `agent_actions` for **Sam SMS** | **No** — three destinations |
| Inbound reply | `REPLY_RECEIVED` → Telegram | Riley classifies replies, updates status, deposits action cards (`rileyAgent.js:1427–1437`) | **No** — Riley is triage + CRM; Warm Routing is Jacob ping |
| Prospect `status=warm` | Not written by Warm Routing | Brevo webhook `checkAndUpdateWarmStatus` (`routes/webhooks.js`) | Different flag |
| Vertical `warm_eligible` | Shared gate | `utils/verticalTiers.js:4–9`; also Riley `:1359` and webhooks `:618` | Shared **policy**, not duplicate routing |

---

## 6. Runtime observation

One production-shaped AMO acquisition mission was executed via the real modules (plan approval → CER `APPROVE_DISCOVERY` → `Scout.discover` → `runDiscoveryPipeline`).

```text
Warm Routing
  ↓  (never required, never called)
Every Warm Routing function executed
  ↓  none
Outputs produced
  ↓  none
Who consumed them
  ↓  nobody
Downstream behavior changed
  ↓  no — Scout still built a Market Definition and blocked on missing providers
```

Disabled-path measurement of `run()` in the same process **after** the mission (explicit require):

| Call | Result | Latency | DB |
|---|---|---|---|
| `run()` with `WARM_ROUTING_ENABLED` unset | `{ disabled: true, reason: 'WARM_ROUTING_ENABLED_not_true', fires: 0 }` | **0.214 ms** | **0** |
| `run()` with `WARM_ROUTING_ENABLED=true` and empty `warm_routing_control` | `{ disabled: true, reason: 'warm_routing_seed_incomplete', fires: 0 }` | **0.054 ms** | 1 `SELECT` on `warm_routing_control` |

---

## 7. Null experiment

Conceptually replace Warm Routing's result with `{}` or `null` (do not modify code).

Walk the **canonical** execution:

```text
CER → ExecutionRouter → advanceDiscoveryAfterApproval → Scout.discover → Market Definition → …
```

**First component that fails: none.** The pipeline never reads Warm Routing output.

This environment already approximates the experiment: `WARM_ROUTING_ENABLED` is unset, so `run()` returns the disabled object and the scheduler never starts (`warmRoutingAgent.js:1105–1106`, `:1271–1274`). AMO discovery still ran.

Operational effects if the worker stayed null forever (not pipeline failures):

1. Jacob stops receiving Telegram warm pings.
2. `capture_inbox` warm-signal counts in Mira context and daily health go to 0 (`miraContext.js:205–209`, `dailyHealth.js:96–105`). Those queries still succeed.
3. Stale Telegram buttons hit `handleWarmTelegramCallback`, find no open fire, and edit the message to “already resolved” (`:1316–1318`) — no throw.

Riley, setter queue, Scout, Emmett, and AMO continue.

---

## 8. Cost attributable solely to Warm Routing

| Cost | On canonical mission path | Disabled `run()` (default) | Enabled + seeded worker (source analysis; not live-fired here) |
|---|---|---|---|
| LLM calls | **0** | **0** | **0** — `warmRoutingAgent.js` has no Anthropic/OpenAI require (observation `agentHasAnthropicRequire: false`) |
| Tokens | **0** | **0** | **0** |
| Discovery provider calls (Places / SerpAPI / Hunter / Prospeo) | **0** | **0** | **0** |
| API calls | **0** | **0** | Telegram `sendMessage` / `editMessageText` / `answerCallbackQuery`; Todoist `POST /tasks` (timeouts 3–8s) |
| Database reads | **0** | **0** | Heavy: prospects≤500 + laterals, `warm_signal_state`, `icp_score_history`, replies, ping counts |
| Database writes | **0** | **0** | `warm_signal_events`, `warm_signal_state`, `warm_trigger_fires`, `capture_inbox`, `mira_warm_capture_log`, `agent_log`, `agent_run_health` |
| Latency added to a mission | **0 ms** (module never loaded) | n/a | n/a — not on the mission clock |
| Worker latency | n/a | **0.214 ms** | Dominated by SQL + Telegram/Todoist, capped by daily 10 pings |

Process-start side effect: `server.js:48` always `require('./warmRoutingAgent')` so the module graph loads at boot, then `startWarmRoutingScheduler()` no-ops when the env flag is not `true`. That is boot I/O, not mission cost, and not tokens.

---

## 9. Architectural comparison

Canonical chain:

```text
Mission → Market Definition → Hypotheses → Investigation → Evidence Requirements
  → Provider Assignment → Evidence Collection → Identity → Understanding
  → Judgment → Recommendation
```

Warm Routing behaviors classified:

| Behavior | Classification |
|---|---|
| Sitting on Mission → Scout | **Unused** on that chain |
| Market / hypothesis / investigation / provider / identity / understanding / judgment | **Dead** relative to cognition (never attempted) |
| `WARM_ROUTING_ENABLED` default-off + seed gate | **Safety** |
| Advisory lock | **Safety** (single worker) |
| Rising-edge `warm_signal_state` (fire once while active) | **Optimization** / **Safety** (anti-reping) |
| Daily Telegram cap 10 + 20:00 ET overflow digest | **Optimization** |
| Coalesce multiple labels into one `warm_trigger_fires` row | **Optimization** |
| `mira_archived` / DNC / bounce exclusion in SQL | **Safety** |
| Vertical `warm_eligible` | **Safety** (shared policy) |
| Telegram ping + keyboard | **Required** for this worker's purpose; **Legacy** relative to canonical cognition |
| Todoist create on button / 24h auto-escalate | **Required** for this worker; **Legacy** vs cognition |
| `capture_inbox` warm_signal pair | **Side Effect** used as a Mira/health **count**, not as cognition |
| `reportAgentRun` / `agent_log` | **Side Effect** (observability) |
| Scheduler require on every `server.js` boot | **Side Effect** |
| Webhook callback handler always registered | **Side Effect** / leftover listener |

---

## 10. First divergence (STOP)

**Unique responsibility inside the canonical cognitive pipeline: none.**

Warm Routing is not a missing Scout stage. Canonical owners already replaced every cognitive role listed in §5.

**First architectural responsibility Warm Routing still uniquely owns** (outside that pipeline):

| | |
|---|---|
| **Responsibility** | Rising-edge **operator incident routing** for already-known, warm-eligible prospects: one Telegram incident per open fire, optional Todoist queue, paired Mira captures, 24h auto-escalation |
| **File** | `warmRoutingAgent.js` |
| **Function** | `processProspectEvents` |
| **Lines** | `925–961` (claim → open-incident coalesce → `sendWarmTelegramMessage` `:657` → `recordFire` `:803`) |
| **Expected** if it were cognition | Some canonical stage (Judgment / Recommendation / ExecutionRouter specialist) would emit operator routing |
| **Actual** | Direct Telegram/Todoist I/O from a cron/scheduler worker with no mission, no CER, no Market Definition |
| **Reason** | No canonical component creates `warm_trigger_fires`, pings `JACOB_TELEGRAM_CHAT_ID`, or opens those Todoist tasks. Riley's overlapping **signal** path writes `agent_actions` for Sam SMS (`rileyAgent.js:1338–1406`), not this incident lifecycle |

Stop. No further divergences enumerated. No fix proposed.

---

## Acceptance answers

| Question | Answer |
|---|---|
| Does Warm Routing still perform unique work? | **Yes, but not cognitive work.** Unique work is operator incident routing (`processProspectEvents`). |
| Which canonical components replaced its responsibilities? | For cognition: Market Definition, Canonical Hypothesis Engine, Investigation Planner, Capability Gate, Provider registries, Scout identity/understanding/judgment. Warm Routing never held those. |
| Is it still on the production path? | **Not on AMO / Mission Engine / chat / Scout.** Boot still `require`s the module. Scheduler is default-off. Cron is registered but blocked unless `warm_routing` is added to `enabled_agents`. Telegram callback handler remains live. |
| Is it consuming tokens? | **No.** |
| Is it consuming LLM calls? | **No.** |
| Is it consuming provider calls? | **No** discovery providers. Telegram/Todoist only when a fire is actually sent. |
| What measurable latency does it add? | **0 ms** on a mission. Disabled `run()` ≈ **0.2 ms**. |
| What measurable cost does it add? | **0** mission cost. Enabled unseeded: one `warm_routing_control` SELECT. |
| Can it be removed? | **From the canonical pipeline: it is already absent; nothing in Scout/AMO breaks.** |
| If not, what is the first thing that breaks? | **Nothing in cognition.** First operational loss: Jacob's Telegram warm pings / Todoist auto-queue / Mira warm-signal counts. |

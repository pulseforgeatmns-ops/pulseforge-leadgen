# SPEC-SCOUT-P0-001: Restore autonomous Anchor Scout replenishment

Status: Root cause identified from source (production read access was not available to the investigating session). Corrective code is implemented and tested locally. Live verification, flag changes and authorization decisions are open operator steps.

Tenant: Anchor Cleaning, tenant `10`. Babrun (tenant `13`) was not used.

## Failure map (code evidence; live state unverified)

The only autonomous Anchor replenishment path is the Max outbound control loop. It runs Scout discovery, enrichment, existing-inventory recovery and AO refill: `runMaxOutboundControlLoop` → `defaultScoutRamp` in `services/maxOutboundControlLoop.js`. The standalone `/cron/scout?client_id=10` route and the weekday `scout_unenriched_enrichment` scheduler are secondary roots.

| # | Blocker | Code location | Stage | Consequence | Class |
|---|---|---|---|---|---|
| B1 | Email recovery maintenance halts the whole Anchor cycle. With `ANCHOR_EMAIL_RECOVERY_MAINTENANCE=true` (or the singleton armed with `manifest=NULL`), `readState` returns `mode='maintenance'`, and the loop returns `halted: anchor_recovery_separate_preparation_approval_required` before it reads inventory. | `services/anchorEmailRecovery.js` `readState`; `services/maxOutboundControlLoop.js:1435-1440` | All stages | No discovery, enrichment, recovery, AO refill or control event. Inventory cannot grow. | Execution blocked by governance (intended) |
| B2 | The research-provider pause rejects every tenant-10 research root. With `ANCHOR_RESEARCH_PROVIDERS_PAUSED=true`, `withResearchProviderScope('10')` throws `anchor_research_provider_paused` on entry. This is the production error that was observed. | `utils/anchorResearchProviderPause.js`; `leadgen.js` `run`; `scoutUnenrichedEnrichmentAgent.js` `run`; `defaultScoutRamp` | Discovery, enrichment, verification | Secondary roots do nothing. In legacy mode the Max cycle crashed (`tenant cycle failed`), which also skipped AO refill and the control event. | Governance (intended) + crash defect (fixed) |
| B3 | Holds were invisible. `/cron/scout` answered `200 {success:true}` before running and only logged the rejection to stdout. The pause throws before `logScoutRun`, so no `agent_log` row was written. The durable stage scheduler classifies B1 as a "safe hold", and no operator item was raised. | `routes/cron.js` `runCronAgent`; `services/governedOutboundStageScheduler.js:45`; `services/governedOutboundControlDispatch.js` `launchControlCycle` | Observability | A governed stop looked the same as "ran, found nothing" and as "never ran". Inventory starvation produced no alert. | Defect (fixed) |
| B4 | The enrichment run log used `status='partial'`, which `agent_log_status_check` rejects. `routes/api.js` installs that constraint (allowed: success, failed, skipped, pending, completed, posted, in_progress). Any batch with one failed row threw at `logRun`, after the other rows were already promoted. | `scoutUnenrichedEnrichmentAgent.js` `runScoped` (former line 138) | Enrichment → clean inventory | The weekday scheduler run "failed" and its summary was lost. Under Max, `runEnrichmentBatches` propagated the error, which aborted the whole replenishment cycle: no discovery, no control event, uncounted promotions. This repeats in any cycle where one row fails. | Defect (fixed) |
| B5 | Sending is closed. `ANCHOR_GOVERNED_OUTBOUND_ENABLED` must be exactly `true`. | `services/governedOutboundTenant.js:49-53` | Governed first touch | No first touches, even with inventory. | Governance (intended) |

The live combination of B1, B2 and B5 in production must be confirmed with the scorecard (below). The bounded-acquisition documentation describes the intended standing state as maintenance armed, research paused and outbound disabled. In that state, Scout cannot replenish Anchor by design. Code changes cannot and must not change that.

## Corrective implementation (this change)

- **B2 crash → governed block.** Before any scoped-research consumption or Scout call, the control loop checks the research pause. If research is paused, it sets `discoveryBlockedReason='anchor_research_provider_paused'`, skips Scout, and still measures inventory and writes its control event.
- **B3 observability.**
  - `recordReplenishmentHold` raises one pending `agent_actions` item (`scout_replenishment_blocked`) per reason for tenant 10. It covers both the maintenance halt and any discovery block while inventory is below target, and is suppressed while an identical item is pending.
  - Paused Scout and enrichment runs write one `agent_log` row with `status='skipped'` and the control reason (`utils/researchProviderBlockLog.js`).
  - `/cron/scout` and `/cron/scout_unenriched_enrichment` answer `{success:false, skipped:true, reason}` instead of `success:true` when paused.
- **B4.** The enrichment run log now writes `status='success'` and puts the failed-row count in `error_msg`. A failed log write cannot discard completed promotions or abort a Max cycle.
- **Scorecard.** `scripts/anchorScoutScorecard.js` is a read-only scorecard covering runs, holds, queue depth, control events, verified inventory, first touches by day, recovery-control shape and the reviewed non-secret flags.

None of these changes grants research, preparation, budget, sending or quarantine authority. Babrun and other tenants keep their prior behavior; holds are tenant-10 only.

## Operator prerequisites (stop boundary)

Restoring autonomous replenishment requires explicit decisions that this change does not make:

1. **Decide whether email recovery maintenance (B1) is still required.**
   - To end it: the recovery-control decision path under SPEC-261/262 (install a scoped-resume manifest with `research` capability, set `ANCHOR_EMAIL_SCOPED_RESUME_ID`), or a reviewed return to legacy mode.
   - To stay in it: Scout can only run through the one-page bounded Places authority (`scripts/scout-bounded/run.cjs`, version 2). That authority is research-only and does not provide sustained replenishment.
2. **Disarm the research pause (B2)** on all app processes, per the SPEC-265 rollout sequence. Max-directed Scout needs this even in scoped resume.
3. **Enable governed outbound (B5)** for first touches. This stays within the existing send governor, AO ownership, suppression and daily caps.

## Acceptance (not yet met)

Functional, five-business-day operational, performance, safety and recovery proof require production access after items 1–3. Run `node scripts/anchorScoutScorecard.js --days 7` in the Railway service console each business day. Treat any `scout_replenishment_blocked` item as the reason a day missed the target.

## Rollback

Revert the commit. There are no migrations, flags or persistent control changes. Rows written to `agent_log` and `agent_actions` are audit data and can stay.

'use strict';
// SPEC-SCOUT-P0-003 scheduler branch. Runs only for Tenant 10, only while the
// email recovery is in maintenance, and only with an open research-only grant.
// It runs Scout discovery and raw queue admission, then returns: the existing
// recovery halt still covers enrichment, preparation, AO refill and outbound.
const grants = require('./scoutResearchOnlyGrant');

// Discovery only needs a positive deficit to run; one page per cycle is
// enforced by the durable page store, spend by the Tenant 10 meter.
const RESEARCH_PLAN = Object.freeze({ shouldReplenish: true, deficit: 20, targetDays: 0, targetInventory: 0,
  cleanInventory: 0, planningDailyCapacity: 0, dispatchCapacityNow: 0, recommendedSafeDailyCapacity: 0 });

async function openGrant(pool) {
  const { rows } = await pool.query(`SELECT to_regclass('public.scout_research_only_grants') IS NOT NULL
    AND to_regclass('public.scout_research_missions') IS NOT NULL AS present`);
  if (!rows[0]?.present) return 'schema_missing';
  return (await pool.query("SELECT search_fingerprint FROM scout_research_only_grants WHERE tenant_id='10' AND revoked_at IS NULL LIMIT 1")).rows[0] || null;
}

async function spendForCycle(pool, claim) {
  const { rows } = await pool.query(`SELECT operation,state,count(*)::int AS attempts,sum(reserved_usd_micros)::int AS micros
    FROM scout_research_google_spend WHERE grant_id=$1 AND cycle_number=$2 GROUP BY operation,state ORDER BY operation,state`,
  [claim.grantId, claim.cycleNumber]);
  return { attempts: rows, reservedUsdMicros: rows.reduce((sum, row) => sum + row.micros, 0) };
}

// Returns null when this branch does not apply (the caller keeps its existing
// behavior), otherwise a complete research-only cycle result or governed halt.
async function runResearchOnlyCycleIfGranted({ pool, store, program, governed, cycleId, now = new Date(), logger = console,
  env = process.env, ramp = null, recordHold }) {
  if (!grants.schedulerEnabled(env) || String(governed?.tenantId) !== '10' || String(program?.tenant_id) !== '10') return null;
  const { loadRecoveryState } = require('./anchorEmailRecovery');
  const recovery = await loadRecoveryState(pool, { now });
  if (recovery.mode !== 'maintenance') return null;
  const open = await openGrant(pool);
  const control = require('./maxOutboundControlLoop');
  const halt = async reason => {
    await recordHold({ reason, detail: 'research_only' });
    return { halted: reason, programId: program.id, recoveryMode: recovery.mode, researchOnly: { status: 'blocked', reason },
      prepareRequested: false, preparedAdded: 0, sent: 0 };
  };
  if (open === 'schema_missing') return halt('research_only_schema_unavailable');
  if (!open) return null;
  const source = await control._test.loadSource(pool, program, '10');
  // An exhausted approved research plan buys nothing; never spend a cycle observing it.
  const backoff = await control.loadScoutDiscoveryBackoff(pool, '10', open.search_fingerprint, now);
  if (backoff?.reason === 'approved_search_plan_exhausted') return halt('scout_approved_search_plan_exhausted');
  let claim;
  try { claim = await grants.claimResearchCycle(pool, { program, source, cycleId, env }); }
  catch (error) {
    // A retried durable run reuses its id: the unique cycle id refuses a replay.
    return halt(error.code === '23505' ? 'research_only_cycle_already_claimed'
      : /^research_only_[a-z_]+$/.test(error.code || '') ? error.code : 'research_only_claim_failed');
  }
  if (!claim) return null;

  let scout = null, failure = null;
  try {
    // Discovery sees only the research mission (program view with the research
    // mission as its source). Candidates go to the retained-candidate ledger.
    scout = await grants.runInResearchScope(claim, () => (ramp || control.defaultScoutRamp)({
      pool, store, program: claim.researchProgram, source: claim.researchSource, plan: RESEARCH_PLAN, logger, governedContext: governed,
      searchFingerprint: claim.manifest.fingerprint, now, cycleId, researchOnly: true, skipVerificationRetry: true,
      researchCandidateSink: Object.freeze({ grantId: claim.grantId, cycleNumber: claim.cycleNumber, researchMissionId: claim.researchMissionId }),
    }), pool);
  } catch (error) { failure = error; }
  const spend = await spendForCycle(pool, claim);
  const result = {
    status: failure ? 'stopped' : 'completed', reason: failure ? (failure.reason || failure.code || 'research_cycle_failed') : null,
    discoveryAttempted: Boolean(scout?.discoveryAttempted), discovery: scout?.discovery?.kind || null,
    discoveryReason: scout?.discovery?.reason || null, researchMissionId: claim.researchMissionId,
    // Accepted = passed admission; retained = a new research ledger row. They
    // differ when an accepted business is already retained or already queued.
    acceptedCandidates: Number(scout?.admission?.fit || 0), evaluatedCandidates: Number(scout?.admission?.evaluated || 0),
    retainedCandidates: Number(scout?.discoveredQueued || 0), queuedForEnrichment: 0,
    candidateLedger: scout?.candidateWork ? { total: scout.candidateWork.total, byState: scout.candidateWork.byState,
      byReason: scout.candidateWork.byReason } : null,
    searchGeneration: Number(scout?.searchGeneration || 0), searchGenerationExhausted: scout?.searchGenerationExhausted === true,
    googleReservedUsdMicros: spend.reservedUsdMicros, googleAttempts: spend.attempts,
    ...(scout?.queryProgression?.researchCoverage ? { researchCoverage: scout.queryProgression.researchCoverage } : {}),
    enrichment: false, verification: false, preparation: false, outbound: false,
  };
  await grants.finishResearchCycle(pool, claim, { completed: !failure, result });
  const researchOnly = { grantId: claim.grantId, cycleNumber: claim.cycleNumber, cyclesRemaining: claim.maxCycles - claim.cycleNumber,
    ...result };
  if (scout) {
    // Same event the discovery backoff and search-generation rotation read, so
    // later cycles advance instead of replaying a handled generation.
    await store.event('max_outbound_control', [program.id, cycleId], {
      programId: program.id, cycleId, researchOnly: true, researchOnlyGrantId: claim.grantId, researchOnlyCycle: claim.cycleNumber,
      sendingEnabled: false, preparationEnabled: false, prepareRequested: false, preparedAdded: 0,
      scoutInvoked: true, scoutSearchFingerprint: claim.manifest.fingerprint,
      scoutSearchGeneration: result.searchGeneration, scoutSearchGenerationExhausted: result.searchGenerationExhausted,
      scoutDiscoveryAttempted: result.discoveryAttempted, scoutQueued: 0, researchRetained: result.retainedCandidates,
      researchMissionId: claim.researchMissionId,
      netCleanInventoryDelta: 0, newCleanInventoryAdded: 0, newPromotions: 0, recoveredExisting: 0,
      googleReservedUsdMicros: spend.reservedUsdMicros, discoveryBlockedReason: result.discoveryReason,
    });
  }
  logger.log?.('[max-outbound-control] research-only cycle', JSON.stringify({ tenant: '10', cycleId, ...researchOnly,
    googleAttempts: undefined }));
  if (failure) {
    await recordHold({ reason: result.reason, detail: `research_only_cycle:${claim.cycleNumber}` });
    return { halted: 'research_only_cycle_stopped', programId: program.id, researchOnly, prepareRequested: false, preparedAdded: 0, sent: 0 };
  }
  return { cycleId, programId: program.id, mode: program.mode, recoveryMode: recovery.mode, researchOnly, scout,
    replenishment: { status: 'research_only', reason: null, newQueueAdmissions: 0, retainedCandidates: result.retainedCandidates },
    prepareRequested: false, preparedAdded: 0, sent: 0 };
}

module.exports = { RESEARCH_PLAN, runResearchOnlyCycleIfGranted };

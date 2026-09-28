#!/usr/bin/env node
'use strict';

/**
 * Anchor governed outbound — single-day operating phases (Sep 28 grant profile).
 *
 * Railway (production shell, DATABASE_URL + secrets already loaded):
 *   node scripts/anchorGovernedOperatingDay.js preflight
 *   node scripts/anchorGovernedOperatingDay.js midday
 *   node scripts/anchorGovernedOperatingDay.js scorecard
 *
 * Optional: --execute false on preflight to observe without Scout side effects.
 */

process.env.DOTENV_CONFIG_QUIET = 'true';
require('dotenv').config({ quiet: true });

const pool = require('../db');
const { runMaxOutboundControlLoop } = require('../services/maxOutboundControlLoop');
const { productionService } = require('../services/governedOutbound');

const NY = 'America/New_York';

function parseArgs(argv = process.argv.slice(2)) {
  const phase = argv[0];
  const options = { execute: true };
  for (let i = 1; i < argv.length; i++) {
    if (argv[i] === '--execute' && argv[i + 1]) {
      options.execute = argv[i + 1] !== 'false';
      i += 1;
    } else if (argv[i] === '--verbose') {
      options.verbose = true;
    } else if (argv[i] === '--help' || argv[i] === '-h') {
      options.help = true;
    }
  }
  if (options.help || !phase) {
    console.log(`Usage:
  node scripts/anchorGovernedOperatingDay.js preflight [--execute true|false]
  node scripts/anchorGovernedOperatingDay.js midday
  node scripts/anchorGovernedOperatingDay.js scorecard`);
    process.exit(options.help ? 0 : 1);
  }
  if (!['preflight', 'midday', 'scorecard'].includes(phase)) {
    throw new Error(`Unknown phase: ${phase}`);
  }
  return { phase, options };
}

function nyLocalDayBounds(now = new Date()) {
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-CA', {
    timeZone: NY,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(now).map(p => [p.type, p.value]));
  const day = `${parts.year}-${parts.month}-${parts.day}`;
  const start = new Date(`${day}T00:00:00-04:00`);
  const end = new Date(start.getTime() + 86400000);
  return { day, start, end };
}

function pickPreflightChecks(result) {
  const em = result.emmett || {};
  const plan = result.plan || {};
  const admission = result.scout?.admission || {};
  return {
    recommendedSafeDailyCapacity: em.recommendedSafeDailyCapacity,
    authorizationLimitedCapacity: em.authorizationLimitedCapacity ?? plan.authorizationLimitedCapacity,
    scheduleLimitedCapacity: em.scheduleLimitedCapacity ?? plan.scheduleLimitedCapacity,
    dispatchCapacityNow: em.dispatchCapacityNow ?? plan.dispatchCapacityNow,
    planningDailyCapacity: em.planningDailyCapacity ?? plan.planningDailyCapacity,
    targetDays: plan.targetDays,
    targetInventory: plan.targetInventory,
    cleanInventory: plan.cleanInventory,
    deficit: plan.deficit,
    governor: em.governor ?? plan.governor,
    healthScore: em.healthScore ?? plan.healthScore,
    shouldReplenish: plan.shouldReplenish,
    scoutInvoked: Boolean(result.scout),
    scoutReplenishmentExecuted: Boolean(result.scout),
    netCleanInventoryDelta: result.inventoryGrowth?.netCleanInventoryDelta ?? null,
    sameCompanyCandidatesAttempted: Number(admission.sameCompanyCandidatesAttempted || 0),
    alternateContactsResolved: Number(admission.alternateContactsResolved || 0),
    alternateContactsVerified: Number(admission.alternateContactsVerified || 0),
    alternateContactsAddedToCleanInventory: Number(admission.alternateContactsAddedToCleanInventory || 0),
    scoutSummary: result.scout ? {
      promoted: result.scout.promoted,
      recoveredExisting: result.scout.recoveredExisting,
      discoveredQueued: result.scout.discoveredQueued,
      yield: result.scout.yield,
      lossBuckets: result.scout.lossBuckets,
    } : null,
    dispatchUnavailableNow: em.dispatchUnavailableNow ?? plan.dispatchUnavailableNow,
    capacityReason: em.capacityReason ?? plan.capacityReason,
  };
}

function assertPreflight(checks, options = {}) {
  const failures = [];
  if (Number(checks.planningDailyCapacity) !== 8) {
    failures.push(`planningDailyCapacity expected 8, got ${checks.planningDailyCapacity}`);
  }
  if (Number(checks.targetDays) !== 3) {
    failures.push(`targetDays expected 3, got ${checks.targetDays}`);
  }
  if (Number(checks.targetInventory) !== 24) {
    failures.push(`targetInventory expected 24, got ${checks.targetInventory}`);
  }
  if (Number(checks.targetInventory) === 0) {
    failures.push('targetInventory must not be 0');
  }
  if (Number(checks.cleanInventory) >= 24) {
    failures.push(`cleanInventory expected < 24 for replenishment day, got ${checks.cleanInventory}`);
  }
  if (Number(checks.deficit) <= 0) {
    failures.push(`deficit expected > 0, got ${checks.deficit}`);
  }
  if (checks.shouldReplenish !== true) {
    failures.push(`shouldReplenish expected true, got ${checks.shouldReplenish}`);
  }
  if (options.requireScout && !checks.scoutInvoked) {
    failures.push('Scout replenishment did not run (must not be gated on dispatchCapacityNow)');
  }
  if (Number(checks.planningDailyCapacity) === 0 && Number(checks.recommendedSafeDailyCapacity) > 0) {
    failures.push('planningDailyCapacity is 0 while Emmett recommends capacity (dispatch-closed regression)');
  }
  return failures;
}

async function loadDayControlEvents(dayBounds) {
  const { rows } = await pool.query(`
    SELECT payload, created_at
    FROM acquisition_outbound_events
    WHERE tenant_id = '10'
      AND event_type = 'max_outbound_control'
      AND created_at >= $1 AND created_at < $2
    ORDER BY created_at ASC
  `, [dayBounds.start.toISOString(), dayBounds.end.toISOString()]);
  return rows.map(r => ({ at: r.created_at, ...(r.payload || {}) }));
}

async function loadSentToday(dayBounds) {
  const { rows } = await pool.query(`
    SELECT count(*)::int AS sent
    FROM acquisition_outbound_items i
    JOIN acquisition_outbound_envelopes e ON e.id = i.envelope_id
    WHERE e.tenant_id = '10'
      AND i.status = 'sent'
      AND i.attempted_at >= $1 AND i.attempted_at < $2
  `, [dayBounds.start.toISOString(), dayBounds.end.toISOString()]);
  return Number(rows[0]?.sent || 0);
}

function largestLossBucket(lossBuckets) {
  if (!lossBuckets || typeof lossBuckets !== 'object') return null;
  let best = null;
  for (const [key, value] of Object.entries(lossBuckets)) {
    const n = Number(value?.count ?? value ?? 0);
    if (!Number.isFinite(n) || n <= 0) continue;
    if (!best || n > best.count) best = { bucket: key, count: n };
  }
  return best;
}

async function runPreflight(options) {
  const result = await runMaxOutboundControlLoop({
    pool,
    execute: options.execute,
    logger: console,
  });
  if (result.halted) {
    return { halted: result.halted, result };
  }
  const checks = pickPreflightChecks(result);
  const failures = assertPreflight(checks, { requireScout: options.execute !== false });
  const status = await productionService(pool).status().catch(err => ({ error: err.code || err.message }));
  return {
    phase: 'preflight',
    ok: failures.length === 0,
    failures,
    capture: checks,
    checks,
    programMode: result.mode,
    inventoryGrowth: result.inventoryGrowth,
    statusSummary: status?.program ? {
      mode: status.program.mode,
      lastTickAt: status.program.last_tick_at,
      lastError: status.program.last_error,
    } : status,
    ...(options.verbose ? { raw: result } : {}),
  };
}

async function runMidday() {
  const dayBounds = nyLocalDayBounds();
  const events = await loadDayControlEvents(dayBounds);
  const first = events[0]?.cleanInventoryBefore ?? events[0]?.cleanInventory ?? null;
  const last = events[events.length - 1] || {};
  const sentToday = await loadSentToday(dayBounds);
  const loss = largestLossBucket(last.lossBuckets);
  return {
    phase: 'midday',
    localDay: dayBounds.day,
    cleanInventory: last.cleanInventoryAfter ?? last.cleanInventory ?? null,
    netCleanInventoryDelta: last.netCleanInventoryDelta ?? null,
    newCleanInventoryAdded: last.newCleanInventoryAdded ?? null,
    newVerifiedPromotions: last.newPromotions ?? last.scoutPromoted ?? null,
    recoveredExisting: last.recoveredExisting ?? last.scoutRecovered ?? null,
    sameCompanyCandidatesAttempted: last.sameCompanyCandidatesAttempted ?? null,
    alternateContactsResolved: last.alternateContactsResolved ?? null,
    alternateContactsVerified: last.alternateContactsVerified ?? null,
    alternateContactsAddedToCleanInventory: last.alternateContactsAddedToCleanInventory ?? null,
    actualSendsToday: sentToday,
    startingCleanInventoryObserved: first,
    replenishmentCycles: events.length,
    largestRecoverableLossBucket: loss,
    lastCycleAt: last.at || (events.length ? events[events.length - 1].created_at : null),
  };
}

async function runScorecard() {
  const dayBounds = nyLocalDayBounds();
  const events = await loadDayControlEvents(dayBounds);
  const sentToday = await loadSentToday(dayBounds);
  const first = events[0] || {};
  const last = events[events.length - 1] || first;
  const starting = first.cleanInventoryBefore ?? first.cleanInventory ?? null;
  const ending = last.cleanInventoryAfter ?? last.cleanInventory ?? starting;
  const grossAdded = events.reduce((sum, e) => sum + Number(e.newCleanInventoryAdded || 0), 0);
  const consumed = Math.max(0, Number(starting || 0) + grossAdded - Number(ending || 0));
  const lastEmmett = last.recommendedSafeDailyCapacity ?? last.emmettCapacity;
  const loss = largestLossBucket(last.lossBuckets);
  return {
    phase: 'scorecard',
    localDay: dayBounds.day,
    startingCleanInventory: starting,
    endingCleanInventory: ending,
    grossNewCleanInventoryAdded: grossAdded,
    prospectsConsumedByOutreach: consumed,
    netInventoryChange: ending != null && starting != null ? ending - starting : null,
    actualFirstTouchSends: sentToday,
    emmettRecommendedCapacity: lastEmmett,
    effectiveDispatchCapacity: last.dispatchCapacityNow ?? null,
    planningDailyCapacity: last.planningDailyCapacity ?? null,
    largestScoutBottleneck: loss,
    replenishmentCycles: events.length,
    success: {
      minimum: sentToday >= 1 && events.some(e => e.scoutInvoked),
      operational: events.some(e => Number(e.netCleanInventoryDelta || 0) > 0),
      strong: Number(ending || 0) > Number(starting || 0) && Number(ending || 0) >= Math.min(24, Number(starting || 0) + grossAdded),
    },
  };
}

async function main() {
  const { phase, options } = parseArgs();
  try {
    let out;
    if (phase === 'preflight') out = await runPreflight(options);
    else if (phase === 'midday') out = await runMidday();
    else out = await runScorecard();
    console.log(JSON.stringify(out, null, 2));
    if (out.failures?.length) process.exitCode = 2;
    if (out.halted) process.exitCode = 1;
  } finally {
    await pool.end();
  }
}

if (require.main === module) {
  main().catch(err => {
    console.error(JSON.stringify({ error: err.code || err.message, stack: err.stack }));
    process.exitCode = 1;
  });
}

module.exports = {
  parseArgs,
  pickPreflightChecks,
  assertPreflight,
  runPreflight,
  runMidday,
  runScorecard,
};

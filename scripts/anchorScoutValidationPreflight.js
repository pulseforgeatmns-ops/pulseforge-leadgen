#!/usr/bin/env node
'use strict';

// SPEC-SCOUT-P0-002 production validation preflight for Anchor (tenant 10).
// Read-only: one READ ONLY transaction, no provider, email, schema or control
// writes. It answers, gate by gate, whether each authorized validation step may
// proceed, and names the exact blocking state when it may not.
//
//   node scripts/anchorScoutValidationPreflight.js [--expect-revision <sha>] [--cycles 8] [--usd 5]
//
// Exit 0: every gate for mission activation AND the discovery window passed.
// Exit 2: at least one gate blocks; nothing was changed.

require('dotenv').config();

const TENANT_ID = '10';
const DETAILS_PER_PAGE = 20;

function arg(argv, name, fallback) {
  const index = argv.indexOf(name);
  return index >= 0 ? argv[index + 1] : fallback;
}

// A failed probe must not abort the shared read-only transaction for the rest.
async function guarded(client, name, work, fallback) {
  await client.query(`SAVEPOINT p_${name}`);
  try {
    const result = await work();
    await client.query(`RELEASE SAVEPOINT p_${name}`);
    return result;
  } catch (error) {
    await client.query(`ROLLBACK TO SAVEPOINT p_${name}`);
    return fallback(error);
  }
}

function gate(step, name, pass, detail) {
  return { step, gate: name, status: pass ? 'pass' : 'blocked', detail };
}

async function evaluate(client, { env = process.env, expectRevision = null, cycles = 8, usdCeiling = 5,
  now = new Date(), deps = {} } = {}) {
  const gates = [];
  const facts = {};
  const { loadRecoveryState } = deps.recovery || require('../services/anchorEmailRecovery');
  const { plan } = deps.successor || require('./activateAnchorInventoryCapacityMission');
  const { facilityBuyerSuccessorMission, ELIGIBLE_SUBSEGMENTS, SERVICE_CITIES }
    = deps.mission || require('./activateAnchorFacilityBuyerMission');
  const { scoutSearchFingerprint } = deps.control || require('../services/maxOutboundControlLoop');
  const memory = deps.memory || require('../services/scoutTenantDiscoveryMemory');

  // Step 1: revision and identity.
  const revision = env.RAILWAY_GIT_COMMIT_SHA || null;
  facts.revision = revision;
  gates.push(gate(1, 'deployed_revision', !expectRevision || revision === expectRevision,
    expectRevision ? `running ${revision || 'unknown'}, expected ${expectRevision}` : `running ${revision || 'unknown'}`));

  // Step 2: successor preview through the audited transaction's own plan.
  const review = await guarded(client, 'plan', () => plan(client, facilityBuyerSuccessorMission), error => {
    gates.push(gate(2, 'successor_preview', false, error.message));
    return null;
  });
  if (review) {
    const current = review.source;
    const alreadyActive = String(current.id).startsWith('mission_anchor_facility_buyers_');
    const successor = review.mission.structuredMission;
    facts.program = { id: review.program.id, mode: review.program.mode, sourceMissionId: current.id };
    facts.currentScope = { eligibleSubsegments: current.payload?.structuredMission?.market?.eligibleSubsegments || null,
      cities: current.payload?.structuredMission?.geography?.cities || null,
      fingerprint: scoutSearchFingerprint(review.program, current) };
    facts.successor = { id: review.mission.id, eligibleSubsegments: successor.market.eligibleSubsegments,
      cities: successor.geography.cities, pendingEnvelopeItems: review.pending, policyHash: review.policyHash,
      fingerprint: scoutSearchFingerprint({ ...review.program, source_mission_id: review.mission.id },
        { id: review.mission.id, payload: review.mission }) };
    gates.push(gate(2, 'successor_scope',
      JSON.stringify(successor.market.eligibleSubsegments) === JSON.stringify(ELIGIBLE_SUBSEGMENTS)
        && JSON.stringify(successor.geography.cities) === JSON.stringify(SERVICE_CITIES),
      `${successor.market.eligibleSubsegments.join(',')} over ${successor.geography.cities.length} cities`));
    gates.push(gate(3, 'not_already_applied', !alreadyActive,
      alreadyActive ? `program already uses ${current.id}; do not re-apply` : `current ${current.id}`));
    gates.push(gate(3, 'prepared_envelope_drained', review.pending === 0,
      `${review.pending} pending/attempted/uncertain envelope item(s)`));
  }

  // Recovery governance: read the same state the control loop reads.
  const recovery = await guarded(client, 'recovery', () => loadRecoveryState(client, { now, env }),
    error => ({ mode: 'maintenance', reason: `recovery_state_unreadable:${error.code || error.message}` }));
  // Maintenance states do not return their manifest; read the row's shape too.
  const control = await guarded(client, 'control', async () => (await client.query(`SELECT maintenance_armed,
    manifest IS NOT NULL AS has_manifest, manifest->>'kind' AS manifest_kind FROM anchor_email_recovery_control`)).rows[0] || null,
  () => null);
  const manifestInstalled = Boolean(recovery.manifest || control?.has_manifest);
  facts.recovery = { mode: recovery.mode, reason: recovery.reason || null,
    maintenanceArmed: control ? control.maintenance_armed === true : null,
    manifestInstalled, manifestKind: recovery.manifest?.kind || control?.manifest_kind || null };
  // Changing the source mission changes the program policy hash; an installed
  // manifest bound to the old policy would fail closed after activation.
  gates.push(gate(3, 'no_recovery_manifest_bound_to_program', !manifestInstalled,
    manifestInstalled ? `installed ${facts.recovery.manifestKind || 'recovery'} manifest is bound to the current program policy` : 'none installed'));

  // Step 5: durable discovery memory, flag and schema.
  const memoryEnabled = memory.isTenantDiscoveryMemoryEnabled(TENANT_ID, env)
    || env.SCOUT_DURABLE_CANDIDATES_ENABLED === 'true';
  facts.discoveryMemory = { flag: memoryEnabled };
  gates.push(gate(5, 'discovery_memory_flag', memoryEnabled,
    `SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED=${env.SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED ?? 'unset'}`));
  const readOnly = { query: (sql, params) => client.query(sql, params), connect: async () => { throw Error('read_only_preflight'); } };
  const schema = await guarded(client, 'memory', async () => {
    await memory.createTenantDiscoveryMemory({ pool: readOnly, tenantId: TENANT_ID, fingerprint: 'preflight' }).assertReady();
    return 'ready';
  }, error => error.code || error.message);
  facts.discoveryMemory.schema = schema;
  gates.push(gate(5, 'discovery_memory_schema', schema === 'ready',
    schema === 'ready' ? 'scout_tenant_query_owners / scout_tenant_place_observations ready' : schema));

  // Step 5: shared provider spend policy actually caps the window.
  const requestCeiling = cycles * (1 + DETAILS_PER_PAGE) + 2;
  const policy = await guarded(client, 'policy', async () => (await client.query(`SELECT paused, daily_request_limit,
    daily_usd_limit FROM api_spend_policies WHERE provider='google_places'`)).rows[0] || null, () => null);
  const spentToday = await guarded(client, 'spend', async () => (await client.query(`SELECT count(*)::int AS calls,
    COALESCE(sum(estimated_usd),0)::float AS usd FROM api_usage_events
    WHERE provider='google_places' AND created_at >= date_trunc('day', NOW())`)).rows[0],
  () => ({ calls: null, usd: null }));
  facts.spend = { policy, todayCalls: spentToday.calls, todayEstimatedUsd: spentToday.usd, requestCeiling, usdCeiling };
  gates.push(gate(5, 'places_spend_policy', Boolean(policy) && policy.paused !== true
    && policy.daily_usd_limit != null && Number(policy.daily_usd_limit) <= usdCeiling
    && policy.daily_request_limit != null && Number(policy.daily_request_limit) <= requestCeiling,
  policy ? `paused=${policy.paused} daily_usd_limit=${policy.daily_usd_limit} daily_request_limit=${policy.daily_request_limit}; need usd<=${usdCeiling}, requests<=${requestCeiling}`
    : 'no google_places row: Places spend is uncapped'));

  // Step 6: the scheduled window itself.
  // Same rules as governedOutboundTenant and anchorResearchProviderPause, read
  // from the environment under evaluation (process.env in production).
  gates.push(gate(6, 'governed_outbound_disabled', env.ANCHOR_GOVERNED_OUTBOUND_ENABLED !== 'true',
    `ANCHOR_GOVERNED_OUTBOUND_ENABLED=${env.ANCHOR_GOVERNED_OUTBOUND_ENABLED ?? 'unset'}`));
  // Max runs Scout only in legacy or scoped_resume. Scoped-resume manifests
  // reject research (resume_paid_research_unsupported), and legacy exists only
  // while the recovery control table is absent: once installed, its singleton
  // stays armed and no flag returns the tenant to legacy.
  facts.recovery.controlTablePresent = Boolean(control);
  gates.push(gate(6, 'recovery_mode_allows_scheduled_research', recovery.mode === 'legacy',
    recovery.mode === 'legacy' ? 'legacy'
      : `${recovery.mode}${recovery.reason ? `:${recovery.reason}` : ''} — Max halts or blocks research in this mode; `
        + (control ? 'the installed recovery control table makes legacy unreachable without removing a permanent control record'
          : 'clear the recovery maintenance/manifest environment to return to legacy')));
  gates.push(gate(6, 'research_providers_unpaused', env.ANCHOR_RESEARCH_PROVIDERS_PAUSED !== 'true',
    `ANCHOR_RESEARCH_PROVIDERS_PAUSED=${env.ANCHOR_RESEARCH_PROVIDERS_PAUSED ?? 'unset'}`));
  gates.push(gate(6, 'max_control_scheduled', env.ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED === 'true',
    `ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED=${env.ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED ?? 'unset'}`));

  const blockedSteps = [...new Set(gates.filter(g => g.status === 'blocked').map(g => g.step))].sort();
  const firstBlocked = blockedSteps[0] ?? null;
  return { tenant: TENANT_ID, generatedAt: now.toISOString(), facts, gates, blockedSteps,
    mayApplyMission: !gates.some(g => g.step <= 3 && g.status === 'blocked'),
    mayStartWindow: !blockedSteps.length,
    verdict: firstBlocked == null ? 'all_gates_pass' : `blocked_at_step_${firstBlocked}` };
}

async function run(pool, options) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN READ ONLY');
    await client.query("SET LOCAL statement_timeout = '15s'");
    return await evaluate(client, options);
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
}

module.exports = { evaluate, run };

if (require.main === module) {
  const argv = process.argv.slice(2);
  const pool = require('../db');
  run(pool, { expectRevision: arg(argv, '--expect-revision', null),
    cycles: Number(arg(argv, '--cycles', 8)), usdCeiling: Number(arg(argv, '--usd', 5)) })
    .then(report => { console.log(JSON.stringify(report, null, 2)); process.exitCode = report.mayStartWindow ? 0 : 2; })
    .catch(error => { console.error(`[anchor-scout-preflight] ${error.message}`); process.exitCode = 1; })
    .finally(() => pool.end().catch(() => {}));
}

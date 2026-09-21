#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — canonical CAPACITY regeneration for a READY mission.
 *
 * Uses REVISE_PREPARED_OUTREACH (SPEC-215/216/217), not GENERATE_CAPACITY from READY.
 * Reuses existing Paige VARIANTS payload; creates a new Emmett CAPACITY contribution.
 * Never calls EXECUTE_OUTBOUND or sends mail.
 *
 * Railway:
 *   node scripts/regenerateAnchorCapacityRevision.js --confirm-production \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750
 *
 * Then rerun the readiness probe unchanged:
 *   node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production
 */

require('dotenv').config();

const amo = require('../packages/acquisition-mission');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  createExecutionRequest,
  routeExecutionRequest,
  findValidExecutionApproval,
  validateProspectMessageBindings,
  isSupersededContribution,
} = amo;
const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const { loadMissionSnapshot } = require('../services/acquisitionMissionPersistence');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const probe = require('./probeAnchorEmmettOutboundReadiness');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const DEFAULT_MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const OPERATOR_ID = 'anchor-capacity-revision';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  const unknown = argv.filter(
    (arg, i) =>
      arg !== '--confirm-production'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--mission-id'
      && (missionIdx < 0 || i !== missionIdx + 1)
  );
  if (unknown.length) {
    throw new Error(
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/regenerateAnchorCapacityRevision.js --confirm-production [--mission-id <id>]`
    );
  }
  if (!missionId || missionId.startsWith('--')) {
    throw new Error('--mission-id requires a mission id value.');
  }
  return { confirmProduction, help, missionId };
}

function printUsage() {
  console.log(`Anchor CAPACITY revision (tenant ${TENANT_ID})

Usage:
  node scripts/regenerateAnchorCapacityRevision.js --confirm-production [--mission-id <id>]

Canonical path:
  REVISE_PREPARED_OUTREACH via ExecutionRouter → advancePreparedOutreachRevision()
  (READY → PREPARE → fresh Paige+CAPACITY → READY; old contributions superseded, not mutated)

Safety:
  Refuses without --confirm-production.
  Reuses existing Paige VARIANTS via runPaige hook (same copy/bindings).
  Never EXECUTE_OUTBOUND. Never enables autosend or changes enabled_agents.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    const err = new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.');
    err.code = 'fixture_fallback_env';
    throw err;
  }
}

function findLatestContribution(contributions, specialist, kind) {
  return [...(contributions || [])]
    .reverse()
    .find((row) => row.specialist === specialist && row.kind === kind) || null;
}

function activePaigePayload(contributions) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.PAIGE
      && row.kind === CONTRIBUTION_KINDS.VARIANTS
      && !isSupersededContribution(row)
  );
  const latest = rows.at(-1) || findLatestContribution(contributions, SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
  if (!latest) return null;
  return unwrapContributionPayload(latest);
}

function activeCapacityRow(contributions) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.EMMETT
      && row.kind === CONTRIBUTION_KINDS.CAPACITY
      && !isSupersededContribution(row)
  );
  return rows.at(-1) || findLatestContribution(contributions, SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY);
}

function inspectCapacitySpec212(payload) {
  const body = unwrapContributionPayload(payload) || {};
  const validation = validateProspectMessageBindings(body);
  return {
    valid: validation.valid === true,
    blocker: validation.valid ? null : (validation.blockerReason || validation.result),
    violationCount: Array.isArray(validation.violations) ? validation.violations.length : 0,
    violations: Array.isArray(validation.violations)
      ? validation.violations.map((v) => ({
        index: v.index,
        reason: v.reason,
        message: v.message,
      }))
      : [],
  };
}

async function ensureExecutionApproval({ runtime, engine, missionId, mission, tenantId }) {
  const snapshot = engine.inspect(missionId, { tenantId });
  if (findValidExecutionApproval(snapshot.contributions || [], missionId)) {
    return { alreadyApproved: true, snapshot };
  }

  const request = createExecutionRequest({
    source: EXECUTION_SOURCES.API,
    intent: EXECUTION_INTENTS.APPROVE_EXECUTION,
    missionId,
    mission,
    operatorId: OPERATOR_ID,
    stage: STAGES.READY,
    question: 'Authorize prepared bundle for revision preflight (no send).',
    permissions: { canExecute: true, role: 'operator' },
  });

  const routed = await routeExecutionRequest(request, {
    engine,
    tenantId,
    operatorId: OPERATOR_ID,
    ...runtime.persistOpts({ persist: true }),
  });

  return {
    alreadyApproved: routed.executionResult?.alreadyExecuted === true,
    snapshot: routed.snapshot || engine.inspect(missionId, { tenantId }),
    action: routed.action,
  };
}

async function runRevision({ runtime, engine, missionId, mission, tenantId, paigePayload }) {
  const request = createExecutionRequest({
    source: EXECUTION_SOURCES.API,
    intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
    missionId,
    mission,
    operatorId: OPERATOR_ID,
    stage: STAGES.READY,
    question: 'Regenerate outbound capacity with current persistence. Reuse Paige variants. Do not send.',
    permissions: { canExecute: true, role: 'operator' },
  });

  return routeExecutionRequest(request, {
    engine,
    tenantId,
    operatorId: OPERATOR_ID,
    ...runtime.persistOpts({ persist: true }),
    runPaige: async () => paigePayload,
  });
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }

  assertRuntimeEnv();

  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool });
  await runtime.hydrate(TENANT_ID, { pool, production: true });
  const engine = runtime.engine();

  const missionId = options.missionId;
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    const err = new Error(`Mission ${missionId} not found for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_found';
    throw err;
  }
  if (mission.stage !== STAGES.READY) {
    const err = new Error(`Mission ${missionId} is at stage ${mission.stage}; revision requires READY.`);
    err.code = 'tme_revision_wrong_stage';
    throw err;
  }

  const beforeSnapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const oldCapacity = activeCapacityRow(beforeSnapshot.contributions || []);
  const paigePayload = activePaigePayload(beforeSnapshot.contributions || []);
  if (!paigePayload || !Array.isArray(paigePayload.variants) || !paigePayload.variants.length) {
    const err = new Error('No reusable Paige VARIANTS contribution found on mission.');
    err.code = 'paige_variants_missing';
    throw err;
  }

  const beforeSpec212 = oldCapacity
    ? inspectCapacitySpec212(oldCapacity.payload)
    : { valid: false, blocker: 'capacity_missing', violationCount: 0, violations: [] };

  const approvalStep = await ensureExecutionApproval({
    runtime,
    engine,
    missionId,
    mission: engine.get(missionId, TENANT_ID),
    tenantId: TENANT_ID,
  });

  const revisionResult = await runRevision({
    runtime,
    engine,
    missionId,
    mission: engine.get(missionId, TENANT_ID),
    tenantId: TENANT_ID,
    paigePayload,
  });
  if (revisionResult.executionResult?.rolledBack === true) {
    const err = new Error(
      revisionResult.executionResult?.error?.message || 'Prepared outreach revision rolled back.'
    );
    err.code = revisionResult.executionResult?.error?.code || 'tme_persistence';
    throw err;
  }

  const persistOpts = runtime.persistOpts({ persist: true });
  const durableSnapshot = persistOpts.pool
    ? await loadMissionSnapshot(missionId, TENANT_ID, persistOpts.pool)
    : null;
  if (!durableSnapshot || !durableSnapshot.mission) {
    const err = new Error('Revision committed in memory but durable mission snapshot is missing.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  const durableCapacities = (durableSnapshot.contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
  );
  const durableOld = oldCapacity
    ? durableCapacities.find((row) => row.id === oldCapacity.id)
    : null;
  const durableNew = activeCapacityRow(durableSnapshot.contributions || [])
    || findLatestContribution(durableSnapshot.contributions, SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY);
  const durablePaige = findLatestContribution(
    durableSnapshot.contributions,
    SPECIALISTS.PAIGE,
    CONTRIBUTION_KINDS.VARIANTS
  );

  if (!durableNew || durableNew.id === oldCapacity?.id) {
    const err = new Error('Revised CAPACITY was not inserted into acquisition_mission_contributions.');
    err.code = 'tme_persistence_verify';
    throw err;
  }
  if (durableOld && !isSupersededContribution(durableOld)) {
    const err = new Error('Old CAPACITY was not durably marked superseded.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  const activeDurableCapacities = durableCapacities.filter((row) => !isSupersededContribution(row));
  if (activeDurableCapacities.length !== 1) {
    const err = new Error(
      `Expected exactly one active CAPACITY after revision; found ${activeDurableCapacities.length}.`
    );
    err.code = 'tme_persistence_verify';
    err.details = {
      activeCapacityIds: activeDurableCapacities.map((row) => row.id),
      allCapacityIds: durableCapacities.map((row) => ({
        id: row.id,
        superseded: isSupersededContribution(row),
      })),
    };
    throw err;
  }
  if (activeDurableCapacities[0].id !== durableNew.id) {
    const err = new Error('Active CAPACITY pointer does not match the newly inserted revision row.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  const afterSpec212 = inspectCapacitySpec212(durableNew.payload);
  const probeReport = await probe.run({ confirmProduction: true, pool });

  return {
    tenantId: TENANT_ID,
    missionId,
    canonicalPath: {
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      handler: 'advancePreparedOutreachRevision',
      route: 'POST /api/v1/amo/missions/:id/execute { intent: REVISE_PREPARED_OUTREACH }',
      note: 'GENERATE_CAPACITY is blocked at READY (tme_wrong_stage). Revision is the supported reprepare path.',
    },
    approvalPreflight: {
      alreadyApproved: approvalStep.alreadyApproved === true,
      action: approvalStep.action || null,
    },
    revision: {
      action: revisionResult.action,
      transactionId: revisionResult.executionResult?.transactionId || null,
      rolledBack: revisionResult.executionResult?.rolledBack === true,
    },
    contributions: {
      supersededCapacityId: oldCapacity?.id || null,
      newCapacityId: durableNew.id,
      reusedPaigeSourceId: findLatestContribution(
        beforeSnapshot.contributions,
        SPECIALISTS.PAIGE,
        CONTRIBUTION_KINDS.VARIANTS
      )?.id || null,
      newPaigeId: durablePaige?.id || null,
      paigeVariantsReused: true,
      durableSuperseded: durableOld ? isSupersededContribution(durableOld) : null,
      activeCapacityCount: activeDurableCapacities.length,
      durableReload: true,
    },
    spec212: {
      before: beforeSpec212,
      after: afterSpec212,
    },
    probe: probeReport,
    safeForOperatorApprovedTestSend:
      afterSpec212.valid === true
      && probeReport.senderReadiness?.sendable === true
      && probeReport.firstBlocker == null
      && durableSnapshot.mission?.stage === STAGES.READY
      && probeReport.capacityContributionId === durableNew.id,
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  DEFAULT_MISSION_ID,
  parseArgs,
  run,
  inspectCapacitySpec212,
  activePaigePayload,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.probe?.firstBlocker ? 2 : 0;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

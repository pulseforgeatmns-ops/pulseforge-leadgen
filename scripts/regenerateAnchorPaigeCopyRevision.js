#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — regenerate Paige VARIANTS + Emmett CAPACITY with fresh
 * customer-facing copy (does NOT reuse prior Paige payload).
 *
 * Uses REVISE_PREPARED_OUTREACH without runPaige hook so runPaigeVariants()
 * executes with the current copy generator and copy safety validator.
 *
 * Railway:
 *   node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 *
 * Then:
 *   node scripts/probeAnchorEmmettOutboundReadiness.js --confirm-production
 *
 * Never EXECUTE_OUTBOUND. Never sends mail.
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
const {
  validatePaigeVariantCopy,
  validatePaigeVariantsPayload,
} = require('../packages/max/workspace/PaigeCopySafety');
const probe = require('./probeAnchorEmmettOutboundReadiness');

const TENANT_ID = '10';
const DEFAULT_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const OPERATOR_ID = 'anchor-paige-copy-revision';
const BLUE_DOOR_PLACE_ID = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';

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
      `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production [--mission-id <id>]`
    );
  }
  if (!missionId || missionId.startsWith('--')) {
    throw new Error('--mission-id requires a mission id value.');
  }
  return { confirmProduction, help, missionId };
}

function printUsage() {
  console.log(`Anchor Paige copy + CAPACITY revision (tenant ${TENANT_ID})

Usage:
  node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production [--mission-id <id>]

Canonical path:
  REVISE_PREPARED_OUTREACH → runPaigeVariants() → runEmmettForAmoMission()
  Prior Paige VARIANTS and CAPACITY contributions are superseded, not mutated.

Safety:
  Refuses without --confirm-production.
  Never EXECUTE_OUTBOUND. Never enables autosend.
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

function activeContribution(contributions, specialist, kind) {
  const rows = (contributions || []).filter(
    (row) => row.specialist === specialist
      && row.kind === kind
      && !isSupersededContribution(row)
  );
  return rows.at(-1) || findLatestContribution(contributions, specialist, kind);
}

function inspectCapacitySpec212(payload) {
  const body = unwrapContributionPayload(payload) || {};
  const validation = validateProspectMessageBindings(body);
  return {
    valid: validation.valid === true,
    blocker: validation.valid ? null : (validation.blockerReason || validation.result),
  };
}

function findBlueDoorVariant(paigePayload) {
  const variants = Array.isArray(paigePayload?.variants) ? paigePayload.variants : [];
  return variants.find(
    (row) => String(row.candidateId || row.placeId || '') === BLUE_DOOR_PLACE_ID
      || /blue door/i.test(String(row.companyName || ''))
  ) || null;
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
    question: 'Authorize prepared bundle for Paige copy revision preflight (no send).',
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

async function runRevision({ runtime, engine, missionId, mission, tenantId }) {
  const request = createExecutionRequest({
    source: EXECUTION_SOURCES.API,
    intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
    missionId,
    mission,
    operatorId: OPERATOR_ID,
    stage: STAGES.READY,
    question: 'Regenerate Paige variants with customer-facing copy safety and rebuild CAPACITY. Do not send.',
    permissions: { canExecute: true, role: 'operator' },
  });

  return routeExecutionRequest(request, {
    engine,
    tenantId,
    operatorId: OPERATOR_ID,
    ...runtime.persistOpts({ persist: true }),
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
  const oldPaige = activeContribution(beforeSnapshot.contributions || [], SPECIALISTS.PAIGE, CONTRIBUTION_KINDS.VARIANTS);
  const oldCapacity = activeContribution(beforeSnapshot.contributions || [], SPECIALISTS.EMMETT, CONTRIBUTION_KINDS.CAPACITY);

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
  if (!durableSnapshot?.mission) {
    const err = new Error('Revision committed in memory but durable mission snapshot is missing.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  const newPaige = activeContribution(
    durableSnapshot.contributions || [],
    SPECIALISTS.PAIGE,
    CONTRIBUTION_KINDS.VARIANTS
  );
  const newCapacity = activeContribution(
    durableSnapshot.contributions || [],
    SPECIALISTS.EMMETT,
    CONTRIBUTION_KINDS.CAPACITY
  );

  if (!newPaige || newPaige.id === oldPaige?.id) {
    const err = new Error('Revised Paige VARIANTS was not inserted.');
    err.code = 'tme_persistence_verify';
    throw err;
  }
  if (!newCapacity || newCapacity.id === oldCapacity?.id) {
    const err = new Error('Revised CAPACITY was not inserted.');
    err.code = 'tme_persistence_verify';
    throw err;
  }

  const paigePayload = unwrapContributionPayload(newPaige.payload || newPaige);
  const copySafety = validatePaigeVariantsPayload(paigePayload);
  const blueDoor = findBlueDoorVariant(paigePayload);
  const blueDoorSafety = blueDoor ? validatePaigeVariantCopy(blueDoor) : null;
  const spec212 = inspectCapacitySpec212(newCapacity.payload || newCapacity);
  const probeReport = await probe.run({ confirmProduction: true, pool });

  return {
    tenantId: TENANT_ID,
    missionId,
    sent: false,
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
      supersededPaigeId: oldPaige?.id || null,
      newPaigeId: newPaige.id,
      supersededCapacityId: oldCapacity?.id || null,
      newCapacityId: newCapacity.id,
      paigeVariantsRegenerated: true,
    },
    copySafety: {
      payloadSafe: copySafety.safe === true,
      blocker: copySafety.safe ? null : copySafety.blocker,
      blueDoorBound: Boolean(blueDoor),
      blueDoorCandidateId: blueDoor?.candidateId || null,
      blueDoorSafe: blueDoorSafety?.safe === true,
    },
    blueDoor: blueDoor ? {
      subject: blueDoor.subject,
      body: blueDoor.body,
      cta: blueDoor.cta,
    } : null,
    spec212,
    probe: probeReport,
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  DEFAULT_MISSION_ID,
  BLUE_DOOR_PLACE_ID,
  parseArgs,
  run,
  findBlueDoorVariant,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = (
        report.copySafety?.payloadSafe === true
        && report.copySafety?.blueDoorSafe === true
        && report.probe?.firstBlocker == null
      ) ? 0 : 2;
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

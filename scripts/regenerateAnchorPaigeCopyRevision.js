#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — regenerate Paige VARIANTS + Emmett CAPACITY with fresh
 * customer-facing copy (does NOT reuse prior Paige payload).
 *
 * Classifies each candidate mission (READY, EXECUTE pre-send, inconsistent approval)
 * and revises independently. Never EXECUTE_OUTBOUND. Never sends mail.
 *
 * Railway:
 *   node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production
 *   node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 */

require('dotenv').config();

const amo = require('../packages/acquisition-mission');
const {
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  createExecutionRequest,
  routeExecutionRequest,
  findValidExecutionApproval,
  validateProspectMessageBindings,
  isSupersededContribution,
  createEvent,
  EVENT_KINDS,
} = amo;
const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const { loadMissionSnapshot } = require('../services/acquisitionMissionPersistence');
const { unwrapContributionPayload } = require('./validateAnchorCanonicalMission');
const {
  validatePaigeVariantsPayload,
} = require('../packages/max/workspace/PaigeCopySafety');
const {
  TENANT_ID,
  CLIENT_ID,
  activeContribution,
  activePaigePayload,
  detectCustomerSend,
  classifyMissionEligibility,
  listAnchorCandidateMissions,
  seedMissionIntoEngine,
  validatePaigeVariantsDoctrine,
  buildAuditEvent,
} = require('./lib/anchorPaigeCopyRevision');
const {
  validateAnchorCopyDoctrine,
  DOCTRINE_BLOCKER,
} = require('../utils/anchorCopyDoctrine');

const DEFAULT_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const OPERATOR_ID = 'anchor-paige-copy-revision';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : null;
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
  if (missionIdx >= 0 && (!missionId || missionId.startsWith('--'))) {
    throw new Error('--mission-id requires a mission id value.');
  }
  return { confirmProduction, help, missionId };
}

function printUsage() {
  console.log(`Anchor Paige copy + CAPACITY revision (tenant ${TENANT_ID})

Usage:
  node scripts/regenerateAnchorPaigeCopyRevision.js --confirm-production [--mission-id <id>]

Without --mission-id, revises all eligible Anchor missions.

Canonical path:
  REVISE_PREPARED_OUTREACH → runPaigeVariants() → runEmmettForAmoMission()
  Prior Paige VARIANTS and CAPACITY contributions are superseded, not mutated.

Safety:
  Refuses without --confirm-production.
  Skips missions with customer-facing sends.
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
  if (typeof validateAnchorCopyDoctrine !== 'function') {
    const err = new Error('anchorCopyDoctrine validation module is unavailable.');
    err.code = 'doctrine_module_unavailable';
    throw err;
  }
}

function inspectCapacitySpec212(payload) {
  const body = unwrapContributionPayload(payload) || {};
  const validation = validateProspectMessageBindings(body);
  return {
    valid: validation.valid === true,
    blocker: validation.valid ? null : (validation.blockerReason || validation.result),
  };
}

async function ensureExecutionApproval({ runtime, engine, missionId, mission, tenantId }) {
  const snapshot = engine.inspect(missionId, { tenantId });
  if (findValidExecutionApproval(snapshot.contributions || [], missionId)) {
    return { alreadyApproved: true, snapshot };
  }
  if (snapshot.mission.pendingOperatorDecision?.kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL) {
    return { alreadyPending: true, snapshot };
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

async function runRevision({
  runtime,
  engine,
  missionId,
  mission,
  tenantId,
  returnToStage = STAGES.READY,
}) {
  const request = createExecutionRequest({
    source: EXECUTION_SOURCES.API,
    intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
    missionId,
    mission,
    operatorId: OPERATOR_ID,
    stage: mission.stage === STAGES.EXECUTE ? STAGES.EXECUTE : STAGES.READY,
    question: 'Regenerate Paige variants with Anchor copy doctrine and rebuild CAPACITY. Do not send.',
    permissions: { canExecute: true, role: 'operator' },
  });

  return routeExecutionRequest(request, {
    engine,
    tenantId,
    operatorId: OPERATOR_ID,
    returnToStage,
    ...runtime.persistOpts({ persist: true }),
  });
}

function recordAuditEvent(engine, missionId, auditKind, extras = {}) {
  const event = createEvent({
    missionId,
    kind: EVENT_KINDS.OPERATOR_EDIT,
    specialist: SPECIALISTS.OPERATOR,
    label: auditKind,
    payload: buildAuditEvent(auditKind, missionId, extras).payload,
  });
  engine.store.addEvent(event);
  return event;
}

async function reviseOneMission({
  runtime,
  engine,
  candidate,
  eligibility,
  pool,
}) {
  const { mission, contributions } = candidate;
  const missionId = mission.id;
  const returnToStage = eligibility.status === 'execute_revision_allowed'
    ? STAGES.EXECUTE
    : STAGES.READY;

  seedMissionIntoEngine(engine, { mission, contributions });
  const hydrated = engine.get(missionId, TENANT_ID);
  if (!hydrated) {
    const err = new Error(`Mission ${missionId} could not be loaded into runtime.`);
    err.code = 'mission_not_hydrated';
    throw err;
  }

  const beforeSnapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const oldPaige = activeContribution(
    beforeSnapshot.contributions || [],
    SPECIALISTS.PAIGE,
    CONTRIBUTION_KINDS.VARIANTS
  );
  const oldCapacity = activeContribution(
    beforeSnapshot.contributions || [],
    SPECIALISTS.EMMETT,
    CONTRIBUTION_KINDS.CAPACITY
  );

  if (returnToStage === STAGES.READY) {
    await ensureExecutionApproval({
      runtime,
      engine,
      missionId,
      mission: engine.get(missionId, TENANT_ID),
      tenantId: TENANT_ID,
    });
  }

  const revisionResult = await runRevision({
    runtime,
    engine,
    missionId,
    mission: engine.get(missionId, TENANT_ID),
    tenantId: TENANT_ID,
    returnToStage,
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
  const doctrine = validatePaigeVariantsDoctrine(paigePayload);
  if (!doctrine.ok) {
    const err = new Error(`Regenerated copy failed Anchor copy doctrine: ${JSON.stringify(doctrine.violations)}`);
    err.code = DOCTRINE_BLOCKER;
    err.violations = doctrine.violations;
    throw err;
  }

  const spec212 = inspectCapacitySpec212(newCapacity.payload || newCapacity);
  const auditKind = eligibility.status === 'repairable_inconsistent_approval'
    ? 'anchor_paige_copy_inconsistent_approval_repaired'
    : (returnToStage === STAGES.EXECUTE
      ? 'anchor_paige_copy_revised_in_execute_before_send'
      : 'anchor_paige_copy_revised_at_ready');
  recordAuditEvent(engine, missionId, auditKind, {
    reason: eligibility.reason,
    return_to_stage: returnToStage,
    new_paige_id: newPaige.id,
    new_capacity_id: newCapacity.id,
  });

  const afterMission = durableSnapshot.mission;
  const pendingApproval = afterMission.pendingOperatorDecision?.kind === OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    || findValidExecutionApproval(durableSnapshot.contributions, missionId) == null;

  return {
    mission_id: missionId,
    stage: afterMission.stage,
    status: eligibility.status,
    result: pendingApproval ? 'revised_pending_approval' : 'revised',
    revision: {
      action: revisionResult.action,
      transactionId: revisionResult.executionResult?.transactionId || null,
      returnToStage,
    },
    contributions: {
      supersededPaigeId: oldPaige?.id || null,
      newPaigeId: newPaige.id,
      supersededCapacityId: oldCapacity?.id || null,
      newCapacityId: newCapacity.id,
    },
    copySafety: {
      payloadSafe: copySafety.safe === true,
      doctrineOk: doctrine.ok === true,
      blocker: copySafety.safe ? null : copySafety.blocker,
    },
    spec212,
    auditEvent: auditKind,
  };
}

function emptySummary() {
  return {
    ready_revised: 0,
    execute_revised_before_send: 0,
    approval_repaired: 0,
    skipped_sent: 0,
    skipped_wrong_stage: 0,
    skipped_inconsistent: 0,
    failed: 0,
  };
}

function bumpSummary(summary, result) {
  if (result.outcome === 'revised') {
    if (result.eligibilityStatus === 'ready_revision_allowed') summary.ready_revised += 1;
    else if (result.eligibilityStatus === 'execute_revision_allowed') summary.execute_revised_before_send += 1;
    else if (result.eligibilityStatus === 'repairable_inconsistent_approval') summary.approval_repaired += 1;
    return;
  }
  if (result.outcome === 'skipped') {
    if (result.eligibilityStatus === 'skip_sent') summary.skipped_sent += 1;
    else if (result.eligibilityStatus === 'skip_wrong_stage') summary.skipped_wrong_stage += 1;
    else if (result.eligibilityStatus === 'skip_inconsistent_approval') summary.skipped_inconsistent += 1;
    return;
  }
  if (result.outcome === 'failed') summary.failed += 1;
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

  const db = options.pool || pool;
  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();

  const candidates = await listAnchorCandidateMissions(db, options.missionId || null);
  if (options.missionId && !candidates.length) {
    const err = new Error(`Mission ${options.missionId} not found for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_found';
    throw err;
  }

  const summary = emptySummary();
  const missions = [];

  for (const candidate of candidates) {
    const { mission, contributions } = candidate;
    const customerSend = await detectCustomerSend(mission.id, db);
    const eligibility = classifyMissionEligibility({ mission, contributions, customerSend });
    const baseRecord = {
      mission_id: mission.id,
      stage: mission.stage,
      status: eligibility.status,
      reason: eligibility.reason,
    };

    if (
      eligibility.status === 'skip_sent'
      || eligibility.status === 'skip_wrong_stage'
      || eligibility.status === 'skip_inconsistent_approval'
    ) {
      bumpSummary(summary, { outcome: 'skipped', eligibilityStatus: eligibility.status });
      missions.push({
        ...baseRecord,
        result: 'skipped',
        message: eligibility.message || null,
        safetyFailures: eligibility.safetyFailures || null,
        sentRecords: eligibility.sentRecords || null,
      });
      continue;
    }

    if (
      eligibility.status !== 'ready_revision_allowed'
      && eligibility.status !== 'execute_revision_allowed'
      && eligibility.status !== 'repairable_inconsistent_approval'
    ) {
      bumpSummary(summary, { outcome: 'skipped', eligibilityStatus: 'skip_wrong_stage' });
      missions.push({ ...baseRecord, result: 'skipped' });
      continue;
    }

    try {
      const revised = await reviseOneMission({
        runtime,
        engine,
        candidate,
        eligibility,
        pool: db,
      });
      bumpSummary(summary, { outcome: 'revised', eligibilityStatus: eligibility.status });
      missions.push(revised);
    } catch (err) {
      bumpSummary(summary, { outcome: 'failed' });
      missions.push({
        ...baseRecord,
        result: 'failed',
        error: {
          code: err.code || null,
          message: err.message,
          violations: err.violations || null,
        },
      });
    }
  }

  return {
    completedAt: new Date().toISOString(),
    client_id: CLIENT_ID,
    tenantId: TENANT_ID,
    missionFilter: options.missionId || null,
    summary,
    missions,
  };
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  OPERATOR_ID,
  parseArgs,
  run,
  classifyMissionEligibility,
  detectCustomerSend,
  validatePaigeVariantsDoctrine,
  reviseOneMission,
  assertRuntimeEnv,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.summary.failed > 0 ? 1 : 0;
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

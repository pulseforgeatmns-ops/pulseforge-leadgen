#!/usr/bin/env node
'use strict';

/**
 * SPEC-252 — Sync Backus effective observe reaction into AO needs_follow_up + research notes.
 *
 * Railway SSH (after cadence annotation + --reEvaluateReactions):
 *   node scripts/syncBackusObserveOperationalFollowUp.js --confirm-production
 */

require('dotenv').config();

const pool = require('../db');
const { DEFAULTS } = require('./auditAnchorOutboundEvidence');
const { loadPreparedOutreachCadence } = require('../services/preparedOutreachArtifactLoader');
const { findPreparedCadenceAnnotation } = require('../services/preparedCadenceAnnotationPersistence');
const {
  syncObserveReactionOperationalFollowUp,
  BACKUS_BUSINESS_NAME,
} = require('../services/observeReactionOperationalSync');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const missionIdx = argv.indexOf('--mission-id');
  const executionIdx = argv.indexOf('--execution-id');
  return {
    confirmProduction,
    missionId: missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULTS.MISSION_ID,
    executionId: executionIdx >= 0 ? argv[executionIdx + 1] : DEFAULTS.EXECUTION_ID,
    clientId: DEFAULTS.CLIENT_ID,
  };
}

async function run(options = {}) {
  const args = options.missionId != null
    ? {
      confirmProduction: options.confirmProduction === true,
      missionId: options.missionId,
      executionId: options.executionId || DEFAULTS.EXECUTION_ID,
      clientId: options.clientId || DEFAULTS.CLIENT_ID,
    }
    : parseArgs();

  if (!args.confirmProduction && !options.missionId) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }

  const execution = await pool.query(
    'SELECT * FROM acquisition_mission_outbound_executions WHERE id = $1 LIMIT 1',
    [args.executionId]
  ).then((r) => r.rows[0]);
  if (!execution) {
    throw Object.assign(new Error(`Execution not found: ${args.executionId}`), { code: 'execution_not_found' });
  }

  const missionRow = await pool.query(
    'SELECT id, tenant_id, client_id, target_segment, stage FROM acquisition_missions WHERE id = $1 LIMIT 1',
    [args.missionId]
  ).then((r) => r.rows[0]);
  if (!missionRow) {
    throw Object.assign(new Error(`Mission not found: ${args.missionId}`), { code: 'mission_not_found' });
  }

  const annotation = await findPreparedCadenceAnnotation(pool, { executionRecordId: execution.id });
  const preparedCadence = await loadPreparedOutreachCadence({
    missionId: args.missionId,
    preparedArtifactRevision: execution.prepared_artifact_revision,
    executionApprovalContributionId: execution.execution_approval_contribution_id,
    executionRecordId: execution.id,
    prospectId: execution.prospect_id,
  }, pool);

  const result = await syncObserveReactionOperationalFollowUp({
    mission: {
      id: missionRow.id,
      tenantId: missionRow.tenant_id,
      clientId: missionRow.client_id || args.clientId,
      targetSegment: missionRow.target_segment,
      stage: missionRow.stage,
    },
    execution,
    preparedCadence,
    annotation: annotation || { id: null },
    clientId: args.clientId,
    businessName: BACKUS_BUSINESS_NAME,
  }, pool);

  const report = {
    spec: 'SPEC-252',
    missionId: args.missionId,
    executionId: args.executionId,
    businessName: BACKUS_BUSINESS_NAME,
    ...result,
  };

  console.log(JSON.stringify(report, null, 2));
  return report;
}

if (require.main === module) {
  run().catch((err) => {
    console.error(JSON.stringify({
      error: err.message,
      code: err.code || 'sync_backus_observe_operational_follow_up_failed',
    }, null, 2));
    process.exit(1);
  });
}

module.exports = { run, parseArgs };

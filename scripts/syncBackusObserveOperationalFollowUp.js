#!/usr/bin/env node
'use strict';

/**
 * SPEC-252 — sync-only production bridge for Backus observe operational follow-up.
 * Reads durable effective observe reactions; never re-evaluates or reruns outbound.
 *
 * Railway SSH:
 *   node scripts/syncBackusObserveOperationalFollowUp.js --confirm-production \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750 \
 *     --execution-id amo_send_37a03a00-2686-4804-8360-9cf93edb52ba
 *
 * Optional:
 *   --dry-run
 */

require('dotenv').config();

const pool = require('../db');
const { DEFAULTS } = require('./auditAnchorOutboundEvidence');
const { syncObserveReactionOperationalFollowUp } = require('../services/observeReactionOperationalSync');

const BACKUS_BUSINESS_NAME = 'Backus, Meyer & Branch, LLP';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const dryRun = argv.includes('--dry-run');
  const missionIdx = argv.indexOf('--mission-id');
  const executionIdx = argv.indexOf('--execution-id');
  return {
    confirmProduction,
    dryRun,
    missionId: missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULTS.MISSION_ID,
    executionId: executionIdx >= 0 ? argv[executionIdx + 1] : DEFAULTS.EXECUTION_ID,
    clientId: DEFAULTS.CLIENT_ID,
    tenantId: DEFAULTS.TENANT_ID,
  };
}

async function run(options = {}) {
  const args = options.missionId != null
    ? {
      confirmProduction: options.confirmProduction === true,
      dryRun: options.dryRun === true,
      missionId: options.missionId,
      executionId: options.executionId || DEFAULTS.EXECUTION_ID,
      clientId: options.clientId || DEFAULTS.CLIENT_ID,
    }
    : parseArgs();

  if (!args.confirmProduction && !options.missionId) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), {
      code: 'confirm_production_required',
    });
  }

  const report = await syncObserveReactionOperationalFollowUp(pool, {
    missionId: args.missionId,
    executionId: args.executionId,
    clientId: args.clientId,
    businessName: BACKUS_BUSINESS_NAME,
    dryRun: args.dryRun,
  });

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

module.exports = { run, parseArgs, BACKUS_BUSINESS_NAME };

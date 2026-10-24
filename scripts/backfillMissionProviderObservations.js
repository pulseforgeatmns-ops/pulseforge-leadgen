#!/usr/bin/env node
'use strict';

/**
 * Backfill acquisition_mission_observations from acquisition_mission_provider_events.
 * Does not resend mail or mutate provider/email evidence.
 *
 * Usage:
 *   node scripts/backfillMissionProviderObservations.js --mission-id <id> [--confirm-production]
 *   node scripts/backfillMissionProviderObservations.js --execution-id <amo_send_...> [--confirm-production]
 */

require('dotenv').config();

const pool = require('../db');
const { backfillMissionObservationsFromProviderEvents } = require('../services/acquisitionMissionProviderObservation');
const { DEFAULTS } = require('./auditAnchorOutboundEvidence');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const readOpt = (flag) => {
    const idx = argv.indexOf(flag);
    if (idx < 0) return null;
    const value = argv[idx + 1];
    if (!value || value.startsWith('--')) {
      throw Object.assign(new Error(`${flag} requires a value.`), { code: 'arg_required' });
    }
    return value;
  };

  if (!confirmProduction) {
    throw Object.assign(new Error('Refusing without --confirm-production.'), { code: 'confirm_production_required' });
  }
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing DATABASE_URL.'), { code: 'runtime_env_missing' });
  }

  return {
    confirmProduction,
    missionId: readOpt('--mission-id') || DEFAULTS.MISSION_ID,
    executionId: readOpt('--execution-id') || DEFAULTS.EXECUTION_ID,
    tenantId: readOpt('--tenant-id') || DEFAULTS.TENANT_ID,
  };
}

async function run(options = {}) {
  const args = options.args || parseArgs();
  const db = options.pool || pool;
  const report = await backfillMissionObservationsFromProviderEvents({
    missionId: args.missionId,
    executionRecordId: args.executionId,
    tenantId: args.tenantId,
  }, db, { persist: true });

  return {
    backfill: 'mission_provider_observations',
    readOnlyExceptObservations: true,
    ...report,
    completedAt: new Date().toISOString(),
  };
}

module.exports = { run, parseArgs };

if (require.main === module) {
  run()
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      const ok = report.observationsCreated > 0
        || report.observationsLinked >= report.providerEventCount
        || report.providerEventCount === 0;
      process.exitCode = ok ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({ error: { code: err.code, message: err.message } }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

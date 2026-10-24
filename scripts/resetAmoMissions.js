#!/usr/bin/env node
'use strict';

/**
 * SPEC-138 — AMO Runtime Production Reset
 *
 * Retires legacy development Acquisition Missions and clears runtime caches.
 * Does NOT touch companies, prospects, discovery profiles, client intelligence,
 * blueprints, scout intelligence, outreach history, or CRM records.
 *
 * Usage:
 *   node scripts/resetAmoMissions.js              # dry-run (counts only)
 *   node scripts/resetAmoMissions.js --apply      # execute reset
 *   node scripts/resetAmoMissions.js --apply --tenant-id 1   # single tenant
 */

require('dotenv').config();

const pool = require('../db');
const { assertAllowed, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');
const { countAmoRows } = require('../services/acquisitionMissionPersistence');
const { resetAmoRuntime } = require('../services/acquisitionMission');

const RESET_VERSION = 'amo-runtime-v1-reset';

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--tenant-id'], flags: ['--apply', '--skip-sessions'] });
  return {
    apply: parsed.flags.has('--apply'),
    tenantId: optionalPositiveInteger(parsed.values.get('--tenant-id'), '--tenant-id'),
    clearSessions: !parsed.flags.has('--skip-sessions'),
  };
}

function sumCounts(counts) {
  return Object.values(counts).reduce((total, n) => total + Number(n || 0), 0);
}

async function run(options = parseArgs(), db = pool) {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is required.');
  }

  const tenantKey = options.tenantId != null ? String(options.tenantId) : null;
  const before = await countAmoRows(tenantKey, db);
  const report = {
    mode: options.apply ? 'apply' : 'dry-run',
    reset_version: RESET_VERSION,
    tenant_id: tenantKey,
    before,
    total_rows: sumCounts(before),
    deleted: null,
    sessions_cleared: 0,
    missions_remaining: null,
    runtime_version: null,
  };

  if (!options.apply) {
    report.message = tenantKey
      ? `Dry-run: would delete ${report.total_rows} AMO rows for tenant ${tenantKey}.`
      : `Dry-run: would delete ${report.total_rows} AMO rows across all tenants.`;
    return report;
  }

  const result = await resetAmoRuntime({
    tenantId: tenantKey,
    pool: db,
    clearSessions: options.clearSessions,
  });

  report.deleted = result.deleted;
  report.sessions_cleared = result.sessionsCleared;
  report.missions_remaining = result.missionsRemaining;
  report.runtime_version = result.runtimeVersion;
  report.after = result.deleted.after;
  report.total_rows_after = sumCounts(result.deleted.after);

  if (report.total_rows_after !== 0) {
    report.error = 'Reset incomplete: AMO tables still contain rows.';
  }

  return report;
}

module.exports = { RESET_VERSION, parseArgs, run };

if (require.main === module) {
  run()
    .then((report) => {
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.error ? 1 : 0;
    })
    .catch((error) => {
      console.error(error.stack || error.message);
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

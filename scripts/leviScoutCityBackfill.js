#!/usr/bin/env node
'use strict';

// Tenant-21 city provenance repair. Dry-run unless --persist is passed.
// Never claims a weekly cycle, never inserts prospects, never enables outbound.

const { inspectMissingCity, backfillMissingCity } = require('../services/leviScoutCityBackfill');

function parseArgs(argv = []) {
  const persist = argv.includes('--persist');
  if (argv.some(arg => arg !== '--persist')) {
    throw Object.assign(new Error(`unknown argument: ${argv.filter(arg => arg !== '--persist').join(' ')}`), {
      code: 'backfill_argument_invalid',
    });
  }
  return { persist };
}

async function run({ db, persist = false } = {}) {
  return persist
    ? backfillMissingCity({ db, persist: true })
    : inspectMissingCity({ db });
}

function publicReport(result) {
  return {
    tenantId: result.tenantId,
    dryRun: result.dryRun !== false,
    inspected: result.inspected,
    missingCity: result.missingCity,
    writable: result.writable,
    alreadySet: result.alreadySet,
    persisted: result.persisted || 0,
    otherTenantWrites: result.otherTenantWrites || 0,
    inferenceSources: (result.plans || []).reduce((counts, plan) => {
      if (!plan.wouldWrite && !plan.beforeCity) return counts;
      const key = plan.inferenceSource || 'none';
      counts[key] = (counts[key] || 0) + 1;
      return counts;
    }, {}),
    sourceHosts: [...new Set((result.plans || []).flatMap(plan => plan.sourceHosts || []))],
  };
}

module.exports = { parseArgs, run, publicReport };

if (require.main === module) {
  Promise.resolve()
    .then(async () => {
      const { persist } = parseArgs(process.argv.slice(2));
      const db = require('../db');
      const result = await run({ db, persist });
      console.log(JSON.stringify(publicReport({ ...result, dryRun: !persist }), null, 2));
    })
    .then(() => process.exit(0))
    .catch((error) => {
      console.error(`[levi-scout-city-backfill] ${error.code || error.message}`);
      process.exit(2);
    });
}

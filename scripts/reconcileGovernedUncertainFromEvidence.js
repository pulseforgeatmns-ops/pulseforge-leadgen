#!/usr/bin/env node
'use strict';

/**
 * Canonical evidence-based reconciliation for governed uncertain sends.
 * Does not invoke transport or mutate items directly to pending.
 *
 *   node scripts/reconcileGovernedUncertainFromEvidence.js --tenant 13 --item ITEM_ID [--dry-run]
 */

require('dotenv').config({ quiet: true });

function parseArgs(argv = process.argv.slice(2)) {
  const options = { dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--tenant') options.tenantId = argv[++i];
    else if (argv[i] === '--item') options.itemId = argv[++i];
    else if (argv[i] === '--dry-run') options.dryRun = true;
    else if (argv[i] === '--help' || argv[i] === '-h') options.help = true;
    else throw new Error(`Unknown argument: ${argv[i]}`);
  }
  return options;
}

async function run(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(`Usage:
  node scripts/reconcileGovernedUncertainFromEvidence.js --tenant 13 --item ITEM_ID [--dry-run]
`);
    return { help: true };
  }
  if (!options.tenantId || !options.itemId) {
    throw Object.assign(new Error('--tenant and --item are required'), { code: 'args_required' });
  }
  const pool = require('../db');
  const service = require('../services/governedOutbound').productionService(pool, { tenantId: options.tenantId });
  return service.reconcileFromEvidence(options.itemId, { dryRun: options.dryRun });
}

if (require.main === module) {
  run().then((result) => {
    console.log(JSON.stringify(result, null, 2));
  }).catch((error) => {
    console.error(JSON.stringify({ error: error.code || error.message }));
    process.exitCode = 1;
  });
}

module.exports = { parseArgs, run };

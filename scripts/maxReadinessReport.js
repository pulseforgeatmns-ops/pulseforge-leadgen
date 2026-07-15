'use strict';

require('dotenv').config();
const pool = require('../db');
const { buildReadinessReport, checkPhase3Readiness } = require('../utils/maxReadiness');
const { assertAllowed, boundedInteger, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id','--since-days'], flags: ['--check'] });
  return {
    check: parsed.flags.has('--check'),
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    sinceDays: boundedInteger(parsed.values.get('--since-days'), '--since-days', { defaultValue: 30, max: 365 }),
  };
}

async function run(options = parseArgs(), db = pool) {
  if (options.check && !options.clientId) throw new Error('--check requires --client-id');
  return options.check ? checkPhase3Readiness(options, db) : buildReadinessReport(options, db);
}

module.exports = { parseArgs, run };

if (require.main === module) {
  const options = parseArgs();
  run(options).then(result => {
    console.log(JSON.stringify(result, null, 2));
    process.exitCode = options.check && !result.ready ? 1 : 0;
  }).catch(error => { console.error(error.stack || error.message); process.exitCode=1; })
    .finally(() => pool.end());
}

'use strict';

require('dotenv').config();
const pool = require('../db');
const { buildEventCoverageReport } = require('../utils/maxEventCoverage');
const { assertAllowed, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id'] });
  return { clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id') };
}

if (require.main === module) {
  buildEventCoverageReport(parseArgs()).then(report => console.log(JSON.stringify(report, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode = 1; })
    .finally(() => pool.end());
}

module.exports = { parseArgs };

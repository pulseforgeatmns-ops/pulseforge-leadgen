'use strict';

require('dotenv').config();
const pool = require('../db');
const { getSequenceState } = require('../utils/maxSequenceState');
const { assertAllowed, optionalPositiveInteger, optionalUuid, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--prospect-id','--client-id'] });
  const prospectId = optionalUuid(parsed.values.get('--prospect-id'), '--prospect-id');
  const clientId = optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id');
  if (!prospectId || !clientId) throw new Error('--prospect-id and --client-id are required');
  return { prospectId, clientId };
}

module.exports = { parseArgs };

if (require.main === module) {
  const options = parseArgs();
  getSequenceState(options.prospectId, options.clientId).then(result => console.log(JSON.stringify(result, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode=1; })
    .finally(() => pool.end());
}

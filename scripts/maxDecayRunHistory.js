'use strict';

require('dotenv').config();
const pool = require('../db');
const { assertAllowed, boundedInteger, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id','--limit'] });
  return {
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 25, max: 250 }),
  };
}

async function run(options = parseArgs(), db = pool) {
  const result = await db.query(`
    SELECT DISTINCT ON (run_id)
      run_id,job_type,mode,status,started_at,completed_at,lock_acquired,client_scope,
      batch_limit,start_cursor,end_cursor,candidates_found,prospects_evaluated,scores_changed,
      downgrade_candidates,recommendations_created,decisions_created,errors,error_stage,
      error_code,error_summary,retryable,operational_effects,deployment_commit,details,recorded_at
    FROM max_decay_run_events
    WHERE ($1::int IS NULL OR client_scope=$1)
    ORDER BY run_id,recorded_at DESC
  `, [options.clientId]);
  return result.rows.sort((a, b) => new Date(b.recorded_at) - new Date(a.recorded_at)).slice(0, options.limit);
}

if (require.main === module) {
  run().then(rows => console.log(JSON.stringify({ rows }, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode = 1; })
    .finally(() => pool.end());
}

module.exports = { parseArgs, run };

'use strict';

require('dotenv').config();
const pool = require('../db');
const { assertAllowed, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

const ROLLBACK_REFERENCE = 'tag:pre-max-orchestration-shadow-2026-07-15;commit:8a6cb43ed04ddb95e5026d3c0305158f85f763bd';
const RECOVERY_REFERENCE = 'sha256:55ac18b5c2f9fa0ae5b3916a9f812bd3ff6e495a2fb1c24b31d73749b5229f9d';

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, { values: ['--client-id','--updated-by'] });
  const clientId = optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id');
  if (!clientId) throw new Error('--client-id is required');
  return { clientId, updatedBy: parsed.values.get('--updated-by') || 'max-shadow-hardening' };
}

async function run(options = parseArgs(), db = pool) {
  const result = await db.query(`
    INSERT INTO max_rollout_readiness_config (
      client_id,phase3_allowlisted,minimum_reviewed_samples,minimum_total_reviews,
      shadow_observation_enabled,minimum_reviews_by_transition,terminal_review_requirement,
      minimum_agreement_rate,maximum_failure_rate,maximum_oscillation_rate,
      rollback_documented,rollback_reference,rollback_reference_verified,
      recovery_snapshot_reference,recovery_snapshot_verified,updated_by,updated_at
    ) VALUES ($1,FALSE,100,100,TRUE,'{}'::jsonb,'every',NULL,NULL,NULL,TRUE,$2,TRUE,$3,FALSE,$4,NOW())
    ON CONFLICT (client_id) DO UPDATE SET
      phase3_allowlisted=FALSE,
      minimum_reviewed_samples=100,
      minimum_total_reviews=100,
      shadow_observation_enabled=TRUE,
      minimum_reviews_by_transition='{}'::jsonb,
      terminal_review_requirement='every',
      rollback_documented=TRUE,
      rollback_reference=EXCLUDED.rollback_reference,
      rollback_reference_verified=TRUE,
      recovery_snapshot_reference=EXCLUDED.recovery_snapshot_reference,
      recovery_snapshot_verified=FALSE,
      updated_by=EXCLUDED.updated_by,
      updated_at=NOW()
    RETURNING *
  `, [options.clientId, ROLLBACK_REFERENCE, RECOVERY_REFERENCE, options.updatedBy]);
  return result.rows[0];
}

if (require.main === module) {
  run().then(row => console.log(JSON.stringify(row, null, 2)))
    .catch(error => { console.error(error.stack || error.message); process.exitCode = 1; })
    .finally(() => pool.end());
}

module.exports = { RECOVERY_REFERENCE, ROLLBACK_REFERENCE, parseArgs, run };

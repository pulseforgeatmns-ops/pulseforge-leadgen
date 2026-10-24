'use strict';

require('dotenv').config();
const path = require('path');
const pool = require('../db');
const { verifyRecoveryArtifact } = require('../utils/maxRecoveryVerification');
const { assertAllowed, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv = process.argv.slice(2)) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed,{values:['--path','--expected-sha256','--client-id','--restore-procedure'],flags:['--record','--durable-storage-verified']});
  return {
    path: parsed.values.get('--path'),
    expectedSha256: parsed.values.get('--expected-sha256'),
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'),'--client-id'),
    restoreProcedurePath: parsed.values.get('--restore-procedure') || path.join(__dirname,'..','docs','max-database-recovery.md'),
    record: parsed.flags.has('--record'),
    durableStorageVerified: parsed.flags.has('--durable-storage-verified'),
  };
}

async function run(options = parseArgs(), db = pool) {
  const report = verifyRecoveryArtifact(options);
  if (options.record) {
    if (!options.clientId) throw new Error('--record requires --client-id');
    await db.query(`
      UPDATE max_rollout_readiness_config SET
        recovery_artifact_found=$2,recovery_hash_verified=$3,recovery_archive_readable=$4,
        recovery_restore_procedure_documented=$5,recovery_durable_storage_verified=$6,
        recovery_snapshot_verified=$7,updated_by='max-recovery-verification',updated_at=NOW()
      WHERE client_id=$1
    `,[options.clientId,report.artifact_found,report.hash_verified,report.archive_readable,
      report.restore_procedure_documented,report.durable_storage_verified,report.fully_verified]);
  }
  return report;
}

module.exports = { parseArgs, run };
if (require.main === module) run().then(r=>console.log(JSON.stringify(r,null,2)))
  .catch(e=>{console.error(e.stack||e.message);process.exitCode=1}).finally(()=>pool.end());

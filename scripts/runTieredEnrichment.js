require('dotenv').config();

const pool = require('../db');
const { run } = require('../tieredEnrichmentAgent');

function parseArgs(argv) {
  const params = {};
  for (const arg of argv) {
    if (arg.startsWith('--client_id=')) params.client_id = arg.split('=')[1];
    if (arg === '--dry-run') params.dryRun = true;
    if (arg === '--bucket-a-only') params.bucketAOnly = true;
  }
  return params;
}

run(parseArgs(process.argv.slice(2)))
  .then(result => {
    console.log(JSON.stringify(result.summary, null, 2));
  })
  .catch(err => {
    console.error(`[runTieredEnrichment] Fatal: ${err.stack || err.message}`);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });

#!/usr/bin/env node
'use strict';
// Read-only: no app bootstrap, migrations, outbound requests or secret output.
const { getApiUsageReport, formatApiUsageBrief } = require('../services/apiUsageService');

function parseArgs(args) {
  const result = { days: 14, json: false, confirm: false, help: false };
  for (let i=0; i<args.length; i++) {
    if (args[i] === '--days') result.days = Number(args[++i]);
    else if (args[i] === '--json') result.json = true;
    else if (args[i] === '--confirm-production') result.confirm = true;
    else if (args[i] === '--help') result.help = true;
    else throw new Error('invalid_arguments');
  }
  if (!Number.isInteger(result.days) || result.days < 1 || result.days > 90) throw new Error('invalid_arguments');
  return result;
}
async function main(args = process.argv.slice(2), env = process.env) {
  const opts = parseArgs(args);
  if (opts.help) { console.log('node scripts/apiUsageAudit.js --days 14 --json --confirm-production\nRead-only application-ledger audit. Days and quota resets use UTC. Provider billing is not reconciled.'); return; }
  if (!opts.confirm) throw new Error('production_confirmation_required');
  if (!env.DATABASE_URL) throw new Error('database_not_configured');
  const { Client } = require('pg');
  const c = new Client({ connectionString: env.DATABASE_URL, connectionTimeoutMillis: 10000 });
  try {
    await c.connect();
    await c.query('BEGIN READ ONLY');
    await c.query("SET LOCAL statement_timeout = '15s'");
    const report = await getApiUsageReport({ db: c, days: opts.days });
    await c.query('COMMIT');
    console.log(opts.json ? JSON.stringify(report, null, 2) : formatApiUsageBrief(report));
  } finally { await c.end().catch(() => {}); }
}
if (require.main === module) main().catch(err => {
  const known = ['invalid_arguments','production_confirmation_required','database_not_configured'];
  console.error(JSON.stringify({ status: 'blocked', error: known.includes(err.message) ? err.message : 'api_usage_audit_unavailable' }));
  process.exitCode = 1;
});
module.exports = { parseArgs, main };

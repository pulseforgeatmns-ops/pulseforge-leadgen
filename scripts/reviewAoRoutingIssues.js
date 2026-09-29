#!/usr/bin/env node
'use strict';

const HELP = `Usage: node scripts/reviewAoRoutingIssues.js [--tenant 10] [--limit 50] [--json]
Read-only report of AO routing issue flags (SPEC-AO-004). Requires DATABASE_URL.`;

function parseArgs(args) {
  const options = { limit: 50, tenantId: null, json: false };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') return { help: true };
    if (arg === '--json') options.json = true;
    else if (arg === '--limit' || arg === '--tenant') {
      const value = args[++i];
      if (!value || value.startsWith('--')) throw new Error(`${arg} requires a value`);
      if (arg === '--limit') options.limit = Number(value);
      else options.tenantId = value;
    } else throw new Error(`Unknown option: ${arg}`);
  }
  return options;
}

async function main(args = process.argv.slice(2)) {
  const options = parseArgs(args);
  if (options.help) {
    console.log(HELP);
    return;
  }

  require('dotenv').config({ quiet: true });
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');

  const pool = require('../db');
  const { listRoutingIssueFlags } = require('../services/aoRoutingIssueFlags');
  const flags = await listRoutingIssueFlags({
    tenantId: options.tenantId,
    limit: options.limit,
  });

  if (options.json) {
    console.log(JSON.stringify(flags, null, 2));
    return;
  }

  console.log(`AO routing issue flags: tenant ${options.tenantId || 'all'} · limit ${options.limit}`);
  console.table(flags.map(row => ({
    created_at: row.created_at,
    ao_user_id: row.ao_user_id,
    prospect_id: row.prospect_id,
    issue_type: row.issue_type,
    notes: row.notes,
    route_observed: row.route_observed ? JSON.stringify(row.route_observed) : '',
    decision_id: row.decision_id,
    session_id: row.session_id,
  })));
}

if (require.main === module) {
  main().catch(err => {
    console.error(err.message);
    process.exitCode = 1;
  }).finally(async () => {
    try {
      const pool = require('../db');
      await pool.end();
    } catch { /* ignore */ }
  });
}

module.exports = { parseArgs, main };

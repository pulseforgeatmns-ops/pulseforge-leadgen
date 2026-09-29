#!/usr/bin/env node
'use strict';

const { reviewOptions } = require('../packages/decision-service/ShadowEventRepository');
const { queryShadowReview } = require('../packages/decision-service/shadowReview');
const HELP = `Usage: npm run decision:review -- [--limit 50] [--tenant 10] [--mismatches | --errors | --warnings] [--json]
Read-only admin/developer report; requires DATABASE_URL. Defaults to the latest 50
stored evaluations across tenants. Counts summarize only the returned sample.
--mismatches selects the latest mismatches; --errors selects error-bearing rows.
--warnings selects high-confidence likely mission-inspection routing warnings.
No routing, approvals, or configuration is changed.`;

function parseArgs(args) {
  const options = { limit: 50, tenantId: null, filter: 'all', json: false };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') return { help: true };
    if (arg === '--json') options.json = true;
    else if (arg === '--limit' || arg === '--tenant') {
      const value = args[++i];
      if (!value || value.startsWith('--')) throw new Error(`${arg} requires a value`);
      if (arg === '--limit') options.limit = Number(value);
      else options.tenantId = value;
    } else if (arg === '--mismatches' || arg === '--errors' || arg === '--warnings') {
      if (options.filter !== 'all') throw new Error('Use only one of --mismatches, --errors, or --warnings');
      options.filter = arg.slice(2);
    } else throw new Error(`Unknown option: ${arg}`);
  }
  reviewOptions(options);
  return options;
}

async function main(args = process.argv.slice(2)) {
  const options = parseArgs(args);
  if (options.help) { console.log(HELP); return; }
  require('dotenv').config({ quiet: true });
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');
  const { createShadowPool } = require('../packages/decision-service/ShadowEventSink');
  const db = createShadowPool();
  db.on('error', () => {});
  try {
    const report = await queryShadowReview(db, options);
    if (options.json) console.log(JSON.stringify(report, null, 2));
    else {
      console.log(`Jev shadow review: latest ${options.limit} ${options.filter}; tenant ${options.tenantId || 'all'}`);
      console.log('Counts cover returned rows only. Warnings are observations for human review.');
      console.log(JSON.stringify(report.summary, null, 2));
      const likely = new Set(report.likely_mission_inspections.map(row => row.decision_id));
      console.table(report.evaluations.map(row => ({
        timestamp: row.timestamp, decision_id: row.decision_id, tenant: row.tenant_id,
        status: row.status, current: `${row.current_route?.route}/${row.current_route?.raw_route}`,
        intent: row.intent, recommended: row.recommended_route, confidence: row.confidence,
        inspection: row.inspection_probability, comparison: row.comparison,
        review: likely.has(row.decision_id) ? 'likely mission inspection' : '',
        errors: (row.errors || []).map(error => error.code).join(', '),
      })));
      console.log('Use --json for all stored fields, mismatches, inspection candidates, and error rows.');
    }
  } catch (error) {
    throw new Error(error.code === '42P01'
      ? 'Apply migrations/2026-09-21-decision-shadow-review.sql before querying.'
      : 'Shadow review query failed; check database access and migration status.');
  } finally { await db.end(); }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { parseArgs, main };

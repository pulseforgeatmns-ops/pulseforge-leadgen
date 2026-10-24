#!/usr/bin/env node
'use strict';

const { reviewOptions } = require('../packages/decision-service/ShadowEventRepository');
const { listShadowEvents } = require('../packages/decision-service/ShadowEventRepository');
const { classifyDecisionMismatch } = require('../packages/decision-service/mismatchClassifier');

const HELP = `Usage: node scripts/reviewDecisionShadowWarnings.js [--limit 50] [--tenant 10] [--json]
Read-only admin/developer report of Jev routing mismatch warnings.
Requires DATABASE_URL. Classifies stored shadow evaluations at query time;
no routing, approvals, or configuration is changed.`;

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
  reviewOptions({ ...options, filter: 'all' });
  return options;
}

function formatCurrentRoute(currentRoute = {}) {
  const route = currentRoute.route || '?';
  const raw = currentRoute.raw_route || currentRoute.pipeline || '?';
  return `${route}/${raw}`;
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
    const rows = await listShadowEvents(db, { ...options, filter: 'all' });
    const warnings = rows
      .map(row => ({ row, warning: classifyDecisionMismatch(row) }))
      .filter(entry => entry.warning != null);
    if (options.json) {
      console.log(JSON.stringify({
        spec: 'SPEC-JEV-003',
        mode: 'shadow_warning_review',
        scope: { limit: options.limit, tenant_id: options.tenantId || null },
        summary: { scanned: rows.length, warnings: warnings.length },
        warnings: warnings.map(({ row, warning }) => ({ ...warning, source: row.source })),
      }, null, 2));
      return;
    }
    console.log('Decision Shadow Warning Candidates\n');
    if (warnings.length === 0) {
      console.log(`No warning candidates in the latest ${rows.length} stored evaluation(s).`);
      return;
    }
    console.table(warnings.map(({ row, warning }) => ({
      created_at: warning.timestamp || row.timestamp,
      decision_id: warning.decision_id,
      tenant: warning.tenant_id,
      mission: warning.mission_id,
      intent: warning.intent,
      confidence: warning.confidence,
      insp_prob: warning.inspection_probability,
      current_route: formatCurrentRoute(warning.current_route),
      jev_route: warning.recommended_route,
      warning_type: warning.warning_type,
    })));
    console.log(`Scanned ${rows.length} row(s); ${warnings.length} warning candidate(s). Use --json for full payloads.`);
  } catch (error) {
    throw new Error(error.code === '42P01'
      ? 'Apply migrations/2026-09-21-decision-shadow-review.sql before querying.'
      : 'Shadow warning review query failed; check database access and migration status.');
  } finally { await db.end(); }
}

if (require.main === module) main().catch(error => { console.error(error.message); process.exitCode = 1; });
module.exports = { parseArgs, main, formatCurrentRoute };

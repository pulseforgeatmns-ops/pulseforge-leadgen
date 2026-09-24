#!/usr/bin/env node
'use strict';

const HELP = `Usage: node scripts/reviewAoDailyCommandCenter.js [--tenant 10] [--ao tony] [--date today|YYYY-MM-DD] [--json]
Read-only operator review of AO Daily Command Center (SPEC-AO-005). Requires DATABASE_URL.`;

function parseArgs(args) {
  const options = { tenantId: null, aoName: null, date: null, json: false };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') return { help: true };
    if (arg === '--json') options.json = true;
    else if (arg === '--tenant' || arg === '--ao' || arg === '--date') {
      const value = args[++i];
      if (!value || value.startsWith('--')) throw new Error(`${arg} requires a value`);
      if (arg === '--tenant') options.tenantId = value;
      else if (arg === '--ao') options.aoName = value;
      else options.date = value;
    } else throw new Error(`Unknown option: ${arg}`);
  }
  return options;
}

function resolveDateInput(dateInput) {
  if (!dateInput || dateInput === 'today') {
    return new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
    }).format(new Date());
  }
  return dateInput;
}

async function resolveAoUser({ tenantId, aoName }, db) {
  if (!aoName) return null;
  const { rows } = await db.query(`
    SELECT id, name, email
    FROM users
    WHERE client_id = $1
      AND role = 'ao'
      AND active = true
      AND LOWER(name) LIKE $2
    ORDER BY id ASC
    LIMIT 1
  `, [Number(tenantId), `%${String(aoName).toLowerCase()}%`]);
  return rows[0] || null;
}

async function listAosForTenant(tenantId, db) {
  const { rows } = await db.query(`
    SELECT id, name, email
    FROM users
    WHERE client_id = $1 AND role = 'ao' AND active = true
    ORDER BY name ASC
  `, [Number(tenantId)]);
  return rows;
}

async function buildReport(options, db = null) {
  const pool = db || require('../db');
  const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
  const { ensureAoProspectRoutingSchema } = require('../utils/aoProspectRoutingSchema');
  const { getCommandCenter } = require('../services/aoCommandCenterService');

  await ensureAoFieldSchema();
  await ensureAoProspectRoutingSchema(pool);

  if (!options.tenantId) throw new Error('--tenant required');

  const dateStr = resolveDateInput(options.date);
  const aos = options.aoName
    ? [await resolveAoUser(options, pool)].filter(Boolean)
    : await listAosForTenant(Number(options.tenantId), pool);

  if (options.aoName && !aos.length) {
    throw new Error(`No AO matching "${options.aoName}" for tenant ${options.tenantId}`);
  }

  const reports = [];
  for (const ao of aos) {
    const payload = await getCommandCenter({
      clientId: Number(options.tenantId),
      aoUserId: ao.id,
      aoUserName: ao.name,
      date: dateStr,
      source: 'operator_review_script',
      db: pool,
    });
    reports.push({ ao, payload });
  }

  return { date: dateStr, tenantId: options.tenantId, reports };
}

function formatTextReport(result) {
  const lines = [`AO Daily Command Center — ${result.date}`, ''];
  for (const { ao, payload } of result.reports) {
    lines.push(`AO: ${ao.name}`, '');
    lines.push('Summary');
    lines.push(`Priority accounts: ${payload.summary.priority_accounts}`);
    lines.push(`Follow-ups due: ${payload.summary.followups_due}`);
    lines.push(`Conversations to continue: ${payload.summary.conversations_to_continue}`);
    lines.push(`Flags open: ${payload.summary.routing_flags_open}`);
    lines.push(`Updates needed: ${payload.summary.updates_needed}`);
    lines.push('');
    lines.push('Top Priority Accounts');
    for (const item of payload.sections.priority_accounts.slice(0, 8)) {
      lines.push(`- ${item.company_name} · ${item.next_action || 'no next action'} · score ${item.priority_score ?? '—'}`);
    }
    lines.push('');
  }
  return lines.join('\n');
}

async function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (err) {
    console.error(err.message);
    console.error(HELP);
    process.exit(1);
  }

  if (options.help) {
    console.log(HELP);
    return;
  }

  try {
    const result = await buildReport(options);
    if (options.json) {
      console.log(JSON.stringify(result, null, 2));
    } else {
      console.log(formatTextReport(result));
    }
  } catch (err) {
    console.error('Review failed:', err.message);
    process.exit(1);
  }
}

if (require.main === module) {
  main();
}

module.exports = {
  parseArgs,
  resolveDateInput,
  buildReport,
  formatTextReport,
};

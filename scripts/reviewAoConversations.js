#!/usr/bin/env node
'use strict';

const HELP = `Usage: node scripts/reviewAoConversations.js [--tenant 10] [--status done] [--limit 50] [--json]
Read-only report of AO Max conversations (SPEC-AO-004). Requires DATABASE_URL.`;

function parseArgs(args) {
  const options = { limit: 50, tenantId: null, status: null, json: false };
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    if (arg === '--help' || arg === '-h') return { help: true };
    if (arg === '--json') options.json = true;
    else if (arg === '--limit' || arg === '--tenant' || arg === '--status') {
      const value = args[++i];
      if (!value || value.startsWith('--')) throw new Error(`${arg} requires a value`);
      if (arg === '--limit') options.limit = Number(value);
      else if (arg === '--tenant') options.tenantId = value;
      else options.status = value;
    } else throw new Error(`Unknown option: ${arg}`);
  }
  return options;
}

async function queryConversations({ tenantId, status, limit }, db = null) {
  const pool = db || require('../db');
  const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
  await ensureAoFieldSchema();
  const params = [];
  const where = ['mode = \'conversation\''];

  if (tenantId != null) {
    params.push(Number(tenantId));
    where.push(`client_id = $${params.length}`);
  }
  if (status) {
    params.push(status);
    where.push(`status = $${params.length}`);
  }
  params.push(Math.min(Number(limit) || 50, 200));

  const { rows } = await pool.query(`
    SELECT
      id,
      client_id,
      ao_owner_id,
      status,
      completed,
      prospect_id,
      mission_id,
      created_at,
      updated_at,
      closed_at,
      reopened_at,
      closed_by,
      reopened_by,
      jsonb_array_length(COALESCE(payload->'messages', '[]'::jsonb)) AS message_count
    FROM ao_max_sessions
    WHERE ${where.join(' AND ')}
    ORDER BY updated_at DESC
    LIMIT $${params.length}
  `, params);

  return rows;
}

async function main(args = process.argv.slice(2)) {
  const options = parseArgs(args);
  if (options.help) {
    console.log(HELP);
    return;
  }

  require('dotenv').config({ quiet: true });
  if (!process.env.DATABASE_URL) throw new Error('DATABASE_URL is required');

  const rows = await queryConversations(options);

  if (options.json) {
    console.log(JSON.stringify(rows, null, 2));
    return;
  }

  console.log(`AO conversations: tenant ${options.tenantId || 'all'} · status ${options.status || 'all'} · limit ${options.limit}`);
  console.table(rows.map(row => ({
    updated_at: row.updated_at,
    conversation_id: row.id,
    ao_owner_id: row.ao_owner_id,
    status: row.status,
    prospect_id: row.prospect_id,
    message_count: row.message_count,
    closed_at: row.closed_at,
    reopened_at: row.reopened_at,
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

module.exports = { parseArgs, main, queryConversations };

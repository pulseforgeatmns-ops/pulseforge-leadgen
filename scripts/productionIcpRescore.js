#!/usr/bin/env node
// Explicitly approved production rescore. It only invokes recalculateICP,
// which updates icp_score and icp_score_history; it never updates status/DNC.
require('dotenv').config({ quiet: true });

const pool = require('../db');
const { recalculateICP } = require('../utils/icpScoring');

function arg(name, fallback) {
  const prefix = `--${name}=`;
  const found = process.argv.find(value => value.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
}

async function mapWithConcurrency(values, limit, mapper) {
  const results = new Array(values.length);
  let next = 0;
  await Promise.all(Array.from({ length: Math.min(limit, values.length) }, async () => {
    while (next < values.length) {
      const index = next++;
      results[index] = await mapper(values[index]);
    }
  }));
  return results;
}

async function main() {
  const clientId = Number(arg('client', '1'));
  const concurrency = Math.max(1, Math.min(Number(arg('concurrency', '8')), 12));
  const offset = Math.max(0, Number(arg('offset', '0')));
  const limit = Math.max(0, Number(arg('limit', '0')));
  const quiet = arg('quiet', 'false') === 'true';
  const reason = 'icp_tiered_b2b_rescore';
  const before = await pool.query(`
    SELECT COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE status IN ('dead', 'disqualified', 'closed'))::int AS inactive_statuses
    FROM prospects WHERE client_id = $1
  `, [clientId]);
  const { rows } = await pool.query(
    `SELECT id FROM prospects WHERE client_id = $1 ORDER BY id ${limit ? 'LIMIT $2 OFFSET $3' : ''}`,
    limit ? [clientId, limit, offset] : [clientId]
  );
  const originalLog = console.log;
  if (quiet) console.log = () => {};
  let complete = 0;
  const results = await mapWithConcurrency(rows, concurrency, async row => {
    const result = await recalculateICP(row.id, { clientId, reason });
    complete++;
    if (!quiet && (complete % 100 === 0 || complete === rows.length)) process.stderr.write(`[rescore] ${complete}/${rows.length}\n`);
    return result;
  });
  console.log = originalLog;
  const after = await pool.query(`
    SELECT COUNT(*)::int AS total,
           COUNT(*) FILTER (WHERE status IN ('dead', 'disqualified', 'closed'))::int AS inactive_statuses,
           COUNT(*) FILTER (WHERE icp_score >= 80)::int AS score_80_plus
    FROM prospects WHERE client_id = $1
  `, [clientId]);
  const audit = await pool.query(`
    SELECT COUNT(*)::int AS rows_written
    FROM icp_score_history
    WHERE reason = $1
  `, [reason]);
  process.stdout.write(`${JSON.stringify({
    client_id: clientId,
    offset,
    limit: limit || null,
    attempted: results.length,
    changed: results.filter(result => result?.changed).length,
    unchanged: results.filter(result => result?.found && !result.changed).length,
    audit_rows_written: Number(audit.rows[0]?.rows_written || 0),
    before: before.rows[0],
    after: after.rows[0],
    deleted: Number(before.rows[0]?.total || 0) - Number(after.rows[0]?.total || 0),
    deactivated: Number(after.rows[0]?.inactive_statuses || 0) - Number(before.rows[0]?.inactive_statuses || 0),
  }, null, 2)}\n`);
}

main().catch(err => {
  console.error(err.stack || err.message);
  process.exitCode = 1;
}).finally(() => pool.end());

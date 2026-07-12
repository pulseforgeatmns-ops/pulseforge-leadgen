#!/usr/bin/env node
// Approval-gated, read-only ICP requalification report. Never updates prospects.
require('dotenv').config({ quiet: true });

const pool = require('../db');
const { getClientConfig } = require('../utils/clientContext');
const { previewRecalculateICP } = require('../utils/icpScoring');
const { normalizeVertical, resolveVerticalTier } = require('../utils/verticalTiers');

function arg(name, fallback) {
  const prefix = `--${name}=`;
  const match = process.argv.find(value => value.startsWith(prefix));
  return match ? match.slice(prefix.length) : fallback;
}

async function mapWithConcurrency(values, limit, mapper) {
  const results = new Array(values.length);
  let cursor = 0;
  async function worker() {
    while (cursor < values.length) {
      const index = cursor++;
      results[index] = await mapper(values[index], index);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, values.length) }, worker));
  return results;
}

async function main() {
  const clientId = Number(arg('client', '1'));
  const concurrency = Math.max(1, Math.min(Number(arg('concurrency', '8')), 16));
  const offset = Math.max(0, Number(arg('offset', '0')));
  const limit = Math.max(0, Number(arg('limit', '0')));
  const quiet = arg('quiet', 'false') === 'true';
  const client = await getClientConfig(clientId);
  if (!client) throw new Error(`Active client ${clientId} was not found`);

  const { rows } = await pool.query(`
    SELECT id, vertical, icp_score
    FROM prospects
    WHERE client_id = $1
    ORDER BY id
    ${limit ? 'LIMIT $2 OFFSET $3' : ''}
  `, limit ? [clientId, limit, offset] : [clientId]);

  const unknownVerticals = new Map();
  for (const row of rows) {
    const resolution = resolveVerticalTier(row.vertical, client);
    if (resolution.tier !== 'unknown') continue;
    const raw = row.vertical == null || String(row.vertical).trim() === '' ? '(blank)' : String(row.vertical);
    unknownVerticals.set(raw, (unknownVerticals.get(raw) || 0) + 1);
  }

  // C/W/unknown are deterministically below 80 after their final clamp, so
  // only A/B rows need the expensive engagement query to produce an exact
  // 80+ requalification count. This still evaluates the full population.
  const eligibleRows = rows.filter(row => {
    const tier = resolveVerticalTier(row.vertical, client);
    return tier.tier === 'A' || tier.tier === 'B';
  });
  let completed = 0;
  const eligiblePreviews = await mapWithConcurrency(eligibleRows, concurrency, async row => {
    const result = await previewRecalculateICP(row.id, { clientId, clientConfig: client });
    completed++;
    if (!quiet && (completed % 100 === 0 || completed === eligibleRows.length)) console.error(`[dry-run] ${completed}/${eligibleRows.length} A/B rows`);
    return { ...row, ...result };
  });
  const previewById = new Map(eligiblePreviews.map(row => [String(row.id), row]));
  const previews = rows.map(row => {
    const preview = previewById.get(String(row.id));
    if (preview) return preview;
    const tier = resolveVerticalTier(row.vertical, client);
    return {
      ...row,
      found: true,
      old_score: row.icp_score == null ? null : Number(row.icp_score),
      new_score: tier.score_ceiling,
      normalized_vertical: tier.vertical,
      tier: tier.tier,
      score_ceiling: tier.score_ceiling,
    };
  });

  const perVertical = new Map();
  for (const row of previews) {
    const raw = row.vertical == null || String(row.vertical).trim() === '' ? '(blank)' : String(row.vertical);
    const normalized = normalizeVertical(row.vertical) || '(blank)';
    const key = `${raw}\u0000${normalized}`;
    const current = perVertical.get(key) || {
      stored_vertical: raw,
      normalized_vertical: normalized,
      tier: row.tier,
      prospects: 0,
      before_80_plus: 0,
      after_80_plus: 0,
    };
    current.prospects++;
    if (Number(row.old_score || 0) >= 80) current.before_80_plus++;
    if (Number(row.new_score || 0) >= 80) current.after_80_plus++;
    perVertical.set(key, current);
  }

  const property = previews.filter(row => normalizeVertical(row.vertical) === 'property_management');
  const report = {
    dry_run: true,
    client_id: clientId,
    offset,
    limit: limit || null,
    total_prospects: previews.length,
    full_engagement_recalculations: eligiblePreviews.length,
    before_80_plus: previews.filter(row => Number(row.old_score || 0) >= 80).length,
    after_80_plus: previews.filter(row => Number(row.new_score || 0) >= 80).length,
    property_management: {
      prospects: property.length,
      before_80_plus: property.filter(row => Number(row.old_score || 0) >= 80).length,
      after_80_plus: property.filter(row => Number(row.new_score || 0) >= 80).length,
    },
    unknown_stored_verticals: [...unknownVerticals.entries()]
      .map(([stored_vertical, count]) => ({ stored_vertical, count }))
      .sort((a, b) => b.count - a.count || a.stored_vertical.localeCompare(b.stored_vertical)),
    per_vertical: [...perVertical.values()]
      .sort((a, b) => b.prospects - a.prospects || a.stored_vertical.localeCompare(b.stored_vertical)),
  };
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
}

main()
  .catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(() => pool.end());

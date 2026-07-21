'use strict';

// Phase A2 / Phase B — shadow comparison for the setter qualification threshold.
//
// Production currently uses TWO thresholds:
//   - visibility promotion gate: icp_score >= 70 (utils/setterVisibility.js)
//   - queue display filter:      icp_score >= 40 (routes/setter.js)
//
// This report measures what changing the queue display filter to 70 would do,
// per tenant, WITHOUT changing anything. Run it, review the delta, and only
// then decide whether to set clients.setter_qualification_threshold.
//
// Phase B deliverable additions (still read-only):
//   - percentage reduction per tenant + totals
//   - vertical distribution of the 40–69 band
//   - historical call dispositions / lifecycle outcomes for scores 40–69
//
// Usage: node scripts/thresholdDeltaReport.js [--json]

const pool = require('../db');
const {
  SETTER_QUEUE_DISPLAY_THRESHOLD,
  SETTER_VISIBILITY_THRESHOLD,
} = require('../utils/qualificationThreshold');

const QUEUE_WHERE = `
  p.source = 'scout'
  AND COALESCE(p.setter_visible, false) = true
  AND COALESCE(p.do_not_contact, false) = false
  AND COALESCE(p.is_synthetic, false) = false
`;

const BAND_WHERE = `
  ${QUEUE_WHERE}
  AND COALESCE(p.icp_score, 0) >= $1
  AND COALESCE(p.icp_score, 0) < $2
`;

function pctReduction(at40, at70) {
  if (!at40) return 0;
  return Math.round(((at40 - at70) / at40) * 1000) / 10;
}

async function run() {
  const asJson = process.argv.includes('--json');
  const low = SETTER_QUEUE_DISPLAY_THRESHOLD;
  const high = SETTER_VISIBILITY_THRESHOLD;

  const { rows } = await pool.query(`
    SELECT
      p.client_id,
      c.name AS client_name,
      c.setter_qualification_threshold AS configured_threshold,
      COUNT(*) FILTER (WHERE COALESCE(p.icp_score, 0) >= $1)::int AS queue_at_current,
      COUNT(*) FILTER (WHERE COALESCE(p.icp_score, 0) >= $2)::int AS queue_at_visibility_threshold,
      ARRAY_AGG(p.id ORDER BY p.icp_score DESC)
        FILTER (WHERE COALESCE(p.icp_score, 0) >= $1 AND COALESCE(p.icp_score, 0) < $2)
        AS would_be_excluded_ids
    FROM prospects p
    LEFT JOIN clients c ON c.id = p.client_id
    WHERE ${QUEUE_WHERE}
    GROUP BY p.client_id, c.name, c.setter_qualification_threshold
    ORDER BY p.client_id
  `, [low, high]);

  const verticals = await pool.query(`
    SELECT
      p.client_id,
      COALESCE(NULLIF(TRIM(p.vertical), ''), 'unknown') AS vertical,
      COUNT(*)::int AS count
    FROM prospects p
    WHERE ${BAND_WHERE}
    GROUP BY p.client_id, COALESCE(NULLIF(TRIM(p.vertical), ''), 'unknown')
    ORDER BY p.client_id, count DESC, vertical
  `, [low, high]);

  // Historical outcomes for the 40–69 band: prefer call_dispositions when the
  // table exists; fall back to activity_log call rows + setter_status.
  let historical = { rows: [] };
  try {
    historical = await pool.query(`
      SELECT
        p.client_id,
        COALESCE(cd.disposition, '(no disposition logged)') AS disposition,
        COUNT(*)::int AS count
      FROM prospects p
      LEFT JOIN LATERAL (
        SELECT disposition
        FROM call_dispositions cd
        WHERE cd.prospect_id = p.id AND cd.client_id = p.client_id
        ORDER BY cd.created_at DESC
        LIMIT 1
      ) cd ON true
      WHERE ${BAND_WHERE}
      GROUP BY p.client_id, COALESCE(cd.disposition, '(no disposition logged)')
      ORDER BY p.client_id, count DESC
    `, [low, high]);
  } catch (err) {
    if (!/call_dispositions|does not exist/i.test(err.message)) throw err;
    historical = await pool.query(`
      SELECT
        p.client_id,
        COALESCE(p.setter_status, 'unknown') AS disposition,
        COUNT(*)::int AS count
      FROM prospects p
      WHERE ${BAND_WHERE}
      GROUP BY p.client_id, COALESCE(p.setter_status, 'unknown')
      ORDER BY p.client_id, count DESC
    `, [low, high]);
  }

  const verticalByClient = new Map();
  for (const row of verticals.rows) {
    if (!verticalByClient.has(row.client_id)) verticalByClient.set(row.client_id, []);
    verticalByClient.get(row.client_id).push({ vertical: row.vertical, count: row.count });
  }
  const outcomesByClient = new Map();
  for (const row of historical.rows) {
    if (!outcomesByClient.has(row.client_id)) outcomesByClient.set(row.client_id, []);
    outcomesByClient.get(row.client_id).push({ disposition: row.disposition, count: row.count });
  }

  const tenants = rows.map(row => {
    const at40 = row.queue_at_current;
    const at70 = row.queue_at_visibility_threshold;
    return {
      client_id: row.client_id,
      client_name: row.client_name,
      configured_threshold: row.configured_threshold,
      queue_count_at_40: at40,
      queue_count_at_70: at70,
      queue_count_at_current_threshold: at40,
      queue_count_at_threshold_70: at70,
      exclusion_delta: at40 - at70,
      percentage_reduction: pctReduction(at40, at70),
      vertical_distribution_40_to_69: verticalByClient.get(row.client_id) || [],
      historical_outcomes_40_to_69: outcomesByClient.get(row.client_id) || [],
      would_be_excluded_prospect_ids: row.would_be_excluded_ids || [],
    };
  });

  const totals = tenants.reduce((acc, t) => {
    acc.queue_count_at_40 += t.queue_count_at_40;
    acc.queue_count_at_70 += t.queue_count_at_70;
    acc.exclusion_delta += t.exclusion_delta;
    return acc;
  }, { queue_count_at_40: 0, queue_count_at_70: 0, exclusion_delta: 0 });
  totals.percentage_reduction = pctReduction(totals.queue_count_at_40, totals.queue_count_at_70);

  const report = {
    generated_at: new Date().toISOString(),
    current_queue_threshold: low,
    visibility_threshold: high,
    note: 'No production change is made by this report. Queue membership stays at the current threshold until explicitly approved.',
    totals,
    tenants,
  };

  if (asJson) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(`Threshold delta report — generated ${report.generated_at}`);
    console.log(`Current queue display threshold: ${report.current_queue_threshold}`);
    console.log(`Visibility promotion threshold:  ${report.visibility_threshold}`);
    console.log(`TOTALS  at>=40: ${totals.queue_count_at_40}  at>=70: ${totals.queue_count_at_70}  delta: ${totals.exclusion_delta}  reduction: ${totals.percentage_reduction}%\n`);
    for (const tenant of report.tenants) {
      console.log(`Client ${tenant.client_id} (${tenant.client_name || 'unknown'})`);
      console.log(`  Queue at >= 40: ${tenant.queue_count_at_40}`);
      console.log(`  Queue at >= 70: ${tenant.queue_count_at_70}`);
      console.log(`  Would be excluded: ${tenant.exclusion_delta} (${tenant.percentage_reduction}%)`);
      if (tenant.vertical_distribution_40_to_69.length) {
        console.log('  Vertical distribution (scores 40–69):');
        for (const v of tenant.vertical_distribution_40_to_69) {
          console.log(`    ${v.vertical}: ${v.count}`);
        }
      }
      if (tenant.historical_outcomes_40_to_69.length) {
        console.log('  Historical outcomes (scores 40–69, latest disposition):');
        for (const o of tenant.historical_outcomes_40_to_69) {
          console.log(`    ${o.disposition}: ${o.count}`);
        }
      }
      if (tenant.would_be_excluded_prospect_ids.length) {
        console.log(`  Affected prospect ids (${tenant.would_be_excluded_prospect_ids.length}): ${tenant.would_be_excluded_prospect_ids.join(', ')}`);
      }
      console.log('');
    }
    console.log(report.note);
  }
  await pool.end();
}

run().catch(err => {
  console.error('threshold delta report failed:', err.message);
  process.exitCode = 1;
});

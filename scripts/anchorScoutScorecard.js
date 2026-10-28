#!/usr/bin/env node
'use strict';

// SPEC-SCOUT-P0-001 read-only Anchor (tenant 10) Scout scorecard and Phase A
// evidence. Runs every query inside one READ ONLY transaction; no provider,
// email, schema or control-row effects. Missing tables are reported, not created.
//
//   node scripts/anchorScoutScorecard.js [--days 7]
//
// Environment flags are reported by name and value only for the reviewed
// non-secret controls below.

const CLIENT_ID = 10;
const CONTROL_FLAGS = [
  'ANCHOR_RESEARCH_PROVIDERS_PAUSED',
  'ANCHOR_EMAIL_RECOVERY_MAINTENANCE',
  'ANCHOR_GOVERNED_OUTBOUND_ENABLED',
  'ANCHOR_GOVERNED_PREPARATION_ENABLED',
  'GOVERNED_OUTBOUND_DURABLE_SCHEDULING_ENABLED',
  'GOVERNED_OUTBOUND_DURABLE_CONTROL_TENANT_IDS',
  'SCOUT_ANCHOR_DISCOVERY_MEMORY_ENABLED',
  'SCOUT_DURABLE_CANDIDATES_ENABLED',
  'ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED',
  'ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED',
];

const QUERIES = {
  // 1, 10: did Scout / enrichment run, and was it blocked, failed or empty?
  agentRuns: `
    SELECT agent_name, action, status, COALESCE(payload->>'reason', error_msg) AS reason,
           count(*)::int AS runs, max(ran_at) AS last_ran_at
      FROM agent_log
     WHERE client_id = $1 AND ran_at > NOW() - ($2::int * INTERVAL '1 day')
       AND agent_name IN ('scout', 'scout_unenriched_enrichment', 'setter_handoff', 'handoff_utility')
     GROUP BY 1, 2, 3, 4 ORDER BY last_ran_at DESC`,
  // 2, 5, 10: per-day discovery output (empty runs report found=0)
  scoutDaily: `
    SELECT date_trunc('day', ran_at AT TIME ZONE 'America/New_York')::date AS day,
           count(*)::int AS runs,
           count(*) FILTER (WHERE status = 'skipped')::int AS skipped,
           count(*) FILTER (WHERE status = 'failed')::int AS failed
      FROM agent_log
     WHERE client_id = $1 AND agent_name = 'scout' AND ran_at > NOW() - ($2::int * INTERVAL '1 day')
     GROUP BY 1 ORDER BY 1 DESC`,
  // 4: enrichment queue depth and retry state
  enrichmentQueue: `
    SELECT COALESCE(source, 'unknown') AS source,
           count(*)::int AS total,
           count(*) FILTER (WHERE COALESCE(enrichment_attempts, 0) = 0)::int AS never_attempted,
           count(*) FILTER (WHERE COALESCE(enrichment_attempts, 0) BETWEEN 1 AND 2)::int AS retrying,
           count(*) FILTER (WHERE COALESCE(enrichment_attempts, 0) >= 3)::int AS exhausted,
           max(last_attempt_at) AS last_attempt_at
      FROM scout_unenriched WHERE client_id = $1 GROUP BY 1 ORDER BY total DESC`,
  // 3, 9: query coverage, backoff and control-cycle outcomes
  controlEvents: `
    SELECT event_type, COALESCE(payload->>'discoveryBlockedReason', payload->>'reason') AS reason,
           count(*)::int AS events, max(created_at) AS last_at,
           sum(COALESCE((payload->>'netCleanInventoryDelta')::int, 0))::int AS net_clean_delta,
           sum(COALESCE((payload->>'newPromotions')::int, 0))::int AS new_promotions
      FROM acquisition_outbound_events
     WHERE tenant_id = '10' AND created_at > NOW() - ($2::int * INTERVAL '1 day') AND $1::int = 10
     GROUP BY 1, 2 ORDER BY last_at DESC`,
  // 6, 7: verified sendable stock and prospects awaiting outreach
  inventory: `
    SELECT count(*)::int AS prospects,
           count(*) FILTER (WHERE created_at > NOW() - ($2::int * INTERVAL '1 day'))::int AS created_in_window,
           count(*) FILTER (WHERE email IS NOT NULL)::int AS with_email,
           count(*) FILTER (WHERE email IS NOT NULL AND email_verified = true
             AND COALESCE(do_not_contact, false) = false)::int AS verified_contactable,
           count(*) FILTER (WHERE COALESCE(do_not_contact, false) = true)::int AS do_not_contact
      FROM prospects WHERE client_id = $1`,
  // 8: first touches by business day
  firstTouches: `
    SELECT date_trunc('day', t.created_at AT TIME ZONE 'America/New_York')::date AS day,
           count(DISTINCT t.prospect_id)::int AS first_touches
      FROM touchpoints t
     WHERE t.client_id = $1 AND t.created_at > NOW() - ($2::int * INTERVAL '1 day')
       AND t.action_type IN ('email_sent', 'sent', 'outbound_email')
       AND NOT EXISTS (SELECT 1 FROM touchpoints p WHERE p.prospect_id = t.prospect_id AND p.client_id = $1
         AND p.action_type IN ('email_sent', 'sent', 'outbound_email') AND p.created_at < t.created_at)
     GROUP BY 1 ORDER BY 1 DESC`,
  // 9: recovery/quarantine singleton (shape only)
  recoveryControl: `
    SELECT maintenance_armed, manifest IS NULL AS manifest_missing, manifest->>'kind' AS manifest_kind
      FROM anchor_email_recovery_control WHERE $1::int = 10 AND $2::int > 0`,
  // Operator holds raised by the control loop
  openHolds: `
    SELECT payload->>'reason' AS reason, status, count(*)::int AS items, max(created_at) AS last_at
      FROM agent_actions
     WHERE client_id = $1 AND action_type = 'scout_replenishment_blocked'
       AND created_at > NOW() - ($2::int * INTERVAL '1 day')
     GROUP BY 1, 2`,
  program: `
    SELECT id, mode, policy->>'dailyCap' AS daily_cap, (policy->>'preparationOnly')::boolean AS preparation_only,
           last_tick_at, last_error
      FROM acquisition_outbound_programs WHERE tenant_id = '10' AND mode <> 'revoked' AND $1::int = 10 AND $2::int > 0
     ORDER BY authorized_at DESC LIMIT 1`,
};

function parseDays(argv) {
  const index = argv.indexOf('--days');
  const days = index >= 0 ? Number(argv[index + 1]) : 7;
  if (!Number.isInteger(days) || days < 1 || days > 60) throw new Error('--days must be an integer from 1 to 60');
  return days;
}

async function collectScorecard(pool, { days = 7, env = process.env } = {}) {
  const client = await pool.connect();
  const report = { tenant: CLIENT_ID, windowDays: days, generatedAt: new Date().toISOString(),
    flags: Object.fromEntries(CONTROL_FLAGS.map(name => [name, env[name] ?? null])),
    revision: env.RAILWAY_GIT_COMMIT_SHA || null, sections: {} };
  try {
    await client.query('BEGIN READ ONLY');
    await client.query("SET LOCAL statement_timeout = '15s'");
    for (const [name, sql] of Object.entries(QUERIES)) {
      await client.query(`SAVEPOINT s_${name}`);
      try {
        report.sections[name] = (await client.query(sql, [CLIENT_ID, days])).rows;
        await client.query(`RELEASE SAVEPOINT s_${name}`);
      } catch (error) {
        await client.query(`ROLLBACK TO SAVEPOINT s_${name}`);
        report.sections[name] = { unavailable: error.code || error.message };
      }
    }
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
  return report;
}

module.exports = { collectScorecard, QUERIES, CONTROL_FLAGS };

if (require.main === module) {
  const pool = require('../db');
  collectScorecard(pool, { days: parseDays(process.argv.slice(2)) })
    .then(report => { console.log(JSON.stringify(report, null, 2)); process.exit(0); })
    .catch(error => { console.error(`[anchor-scout-scorecard] ${error.message}`); process.exit(1); });
}

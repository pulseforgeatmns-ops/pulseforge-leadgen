'use strict';

/**
 * SPEC-117 — build an inbox intelligence snapshot from live client data.
 */

const { getWarmupProgress, resolveWarmupDailyCap } = require('../utils/sendWarmup');
const { localDateOf } = require('../packages/emmett-outbound');

function pct(part, whole) {
  const w = Number(whole || 0);
  if (w <= 0) return 0;
  return Number(part || 0) / w;
}

function daysBetween(from, now) {
  if (!from) return 0;
  const start = from instanceof Date ? from : new Date(from);
  if (Number.isNaN(start.getTime())) return 0;
  return Math.max(0, (now.getTime() - start.getTime()) / 86400000);
}

async function querySafe(pool, sql, params, fallback) {
  try {
    return await pool.query(sql, params);
  } catch (err) {
    if (err.code === '42P01' || err.code === '42703') return fallback;
    throw err;
  }
}

async function buildInboxSnapshot(clientId, opts = {}) {
  const pool = opts.pool;
  const now = opts.now instanceof Date ? opts.now : new Date(opts.now || Date.now());
  const timeZone = opts.timeZone || 'America/New_York';
  const tenantId = String(clientId);
  const localDate = opts.localDate || localDateOf(now, timeZone);

  const clientRes = await querySafe(
    pool,
    `SELECT id, sender_email, sending_domain, created_at, warmup_start_date, autosend_enabled
     FROM clients WHERE id = $1`,
    [clientId],
    { rows: [] }
  );
  const client = clientRes.rows[0] || { id: clientId };

  const events = await querySafe(
    pool,
    `SELECT
        COUNT(*) FILTER (WHERE event_type IN ('sent','delivered'))::int AS sends,
        COUNT(*) FILTER (WHERE event_type IN ('hard_bounce','soft_bounce','blocked'))::int AS bounces,
        COUNT(*) FILTER (WHERE event_type IN ('opened','open'))::int AS opens,
        COUNT(*) FILTER (WHERE event_type IN ('replied','reply'))::int AS replies,
        COUNT(*) FILTER (WHERE event_type IN ('spam','complaint'))::int AS complaints,
        MIN(event_at) FILTER (WHERE event_type IN ('sent','delivered')) AS first_sent_at
      FROM email_events
      WHERE client_id = $1
        AND event_at >= NOW() - INTERVAL '7 days'`,
    [clientId],
    { rows: [{ sends: 0, bounces: 0, opens: 0, replies: 0, complaints: 0, first_sent_at: null }] }
  );
  const stats = events.rows[0] || {};

  const todayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_today
       FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND client_id = $1
        AND (ran_at AT TIME ZONE $3)::date = $2::date`,
    [clientId, localDate, timeZone],
    { rows: [{ sent_today: 0 }] }
  );

  const yesterdayRes = await querySafe(
    pool,
    `SELECT COUNT(*)::int AS sent_yesterday
       FROM agent_log
      WHERE agent_name = 'emmett'
        AND action = 'email_sent'
        AND client_id = $1
        AND (ran_at AT TIME ZONE $3)::date = ($2::date - INTERVAL '1 day')`,
    [clientId, localDate, timeZone],
    { rows: [{ sent_yesterday: 0 }] }
  );

  const avgRes = await querySafe(
    pool,
    `SELECT COALESCE(AVG(daily_count), 0)::float AS historical_daily_avg
       FROM (
         SELECT COUNT(*)::int AS daily_count
           FROM agent_log
          WHERE agent_name = 'emmett'
            AND action = 'email_sent'
            AND client_id = $1
            AND ran_at >= NOW() - INTERVAL '14 days'
          GROUP BY (ran_at AT TIME ZONE $2)::date
       ) days`,
    [clientId, timeZone],
    { rows: [{ historical_daily_avg: 0 }] }
  );

  let warmupProgress = { activeSendDays: 0, reset: true };
  try {
    warmupProgress = await getWarmupProgress(pool, clientId, 7, now);
  } catch (_) {
    warmupProgress = { activeSendDays: 0, reset: true };
  }

  const warmupStages = opts.warmupStages || [];
  const warmupCap = resolveWarmupDailyCap(warmupStages, warmupProgress.activeSendDays);
  let warmupStatus = 'none';
  if (warmupStages.length) {
    if (warmupProgress.reset) warmupStatus = 'warming';
    else if (warmupCap && warmupCap < (opts.providerCeiling || 50)) warmupStatus = 'warming';
    else warmupStatus = 'healthy';
  } else if (Number(warmupProgress.activeSendDays || 0) >= 14) {
    warmupStatus = 'healthy';
  }

  const firstSent = stats.first_sent_at || client.warmup_start_date || client.created_at;
  const inboxAgeDays = Math.round(daysBetween(firstSent, now) || Number(opts.inboxAgeDays || 0));

  const auth = opts.authentication || {
    spf: Boolean(client.sending_domain),
    dkim: Boolean(client.sending_domain),
    dmarc: client.sending_domain ? 'none' : false,
  };

  const sends = Number(stats.sends || 0);
  return {
    tenantId,
    clientId,
    inboxId: client.sender_email || `client-${clientId}`,
    domain: client.sending_domain || null,
    localDate,
    timeZone,
    inboxAgeDays,
    providerCeiling: Number(opts.providerCeiling || 50),
    authentication: auth,
    warmup: {
      status: warmupStatus,
      dailyCap: warmupCap || opts.providerCeiling || 50,
      activeSendDays: warmupProgress.activeSendDays,
      reset: warmupProgress.reset,
    },
    bounceRate: pct(stats.bounces, sends),
    replyRate: pct(stats.replies, sends),
    openRate: pct(stats.opens, sends),
    complaintRate: pct(stats.complaints, sends),
    blacklist: opts.blacklist || { listed: false, sources: [] },
    sentToday: Number(todayRes.rows[0]?.sent_today || 0),
    sentYesterday: Number(yesterdayRes.rows[0]?.sent_yesterday || 0),
    historicalDailyAvg: Number(avgRes.rows[0]?.historical_daily_avg || 0),
    recentSends: sends,
    operatorOverride: opts.operatorOverride || null,
    replyByWeekday: opts.replyByWeekday || {},
  };
}

async function loadQueueProspects(clientId, pool, limit = 80) {
  const res = await querySafe(
    pool,
    `SELECT
        p.id, p.email, p.vertical, p.icp_score, p.do_not_contact,
        c.name AS company,
        (
          SELECT MAX(t.created_at)
            FROM touchpoints t
           WHERE t.prospect_id = p.id
             AND t.client_id = p.client_id
             AND t.action_type IN ('open','email_opened','click','email_clicked','hot_flag','inbound_reply')
        ) AS buying_signal_at
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1
        AND COALESCE(p.do_not_contact, FALSE) = FALSE
        AND p.email IS NOT NULL AND p.email <> ''
        AND p.status IN ('cold','contacted','warm')
      ORDER BY p.icp_score DESC NULLS LAST
      LIMIT $2`,
    [clientId, limit],
    { rows: [] }
  );
  return res.rows.map((row) => ({
    id: row.id,
    email: row.email,
    vertical: row.vertical,
    company: row.company,
    icpScore: Number(row.icp_score || 0),
    maxPriority: Number(row.icp_score || 0) / 100,
    buyingSignalAt: row.buying_signal_at,
    expectedResponse: row.buying_signal_at ? 0.12 : 0.05,
    dnc: row.do_not_contact === true,
    paige: null,
    contentSource: null,
  }));
}

module.exports = {
  buildInboxSnapshot,
  loadQueueProspects,
};

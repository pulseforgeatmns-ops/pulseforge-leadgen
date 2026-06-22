const pool = require('../db');

const SEND_EVENTS = "('delivered', 'request')";

function number(value) {
  const parsed = Number(value || 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function rounded(value, places) {
  return Number(number(value).toFixed(places));
}

async function computeDailyHealth({ now = new Date(), query = pool.query.bind(pool) } = {}) {
  const asOf = now instanceof Date ? now : new Date(now);
  if (Number.isNaN(asOf.getTime())) throw new TypeError('computeDailyHealth now must be a valid date');

  const safeRows = async (sql, params = [asOf]) => {
    try {
      const result = await query(sql, params);
      return result?.rows || [];
    } catch (err) {
      console.warn(`[daily_health] query unavailable: ${err.message}`);
      return [];
    }
  };

  const [emailRows, scoutRows, warmRows, errorRows, clientRows] = await Promise.all([
    safeRows(`
      /* daily_health:email */
      SELECT
        COUNT(*) FILTER (
          WHERE event_type IN ${SEND_EVENTS}
            AND event_at >= $1::timestamptz - INTERVAL '24 hours'
            AND event_at < $1::timestamptz
        )::int AS send_count_today,
        COUNT(*) FILTER (
          WHERE event_type IN ${SEND_EVENTS}
            AND event_at >= $1::timestamptz - INTERVAL '8 days'
            AND event_at < $1::timestamptz - INTERVAL '24 hours'
        )::numeric / 7 AS send_count_baseline_7d,
        COUNT(*) FILTER (
          WHERE event_type = 'bounce'
            AND event_at >= $1::timestamptz - INTERVAL '24 hours'
            AND event_at < $1::timestamptz
        )::int AS bounce_count_today,
        COUNT(*) FILTER (
          WHERE event_type IN ('replied', 'reply')
            AND event_at >= $1::timestamptz - INTERVAL '24 hours'
            AND event_at < $1::timestamptz
            AND LOWER(COALESCE(raw_payload->>'classification', '')) <> 'out_of_office'
        )::int AS reply_count_today
      FROM email_events
      WHERE event_at >= $1::timestamptz - INTERVAL '8 days'
        AND event_at < $1::timestamptz
    `),
    safeRows(`
      /* daily_health:scout */
      SELECT
        COUNT(*) FILTER (
          WHERE created_at >= $1::timestamptz - INTERVAL '24 hours'
            AND created_at < $1::timestamptz
        )::int AS scout_prospects_added_today,
        COUNT(*) FILTER (
          WHERE created_at >= $1::timestamptz - INTERVAL '8 days'
            AND created_at < $1::timestamptz - INTERVAL '24 hours'
        )::numeric / 7 AS scout_baseline_7d
      FROM prospects
      WHERE created_at >= $1::timestamptz - INTERVAL '8 days'
        AND created_at < $1::timestamptz
    `),
    safeRows(`
      /* daily_health:warm_signals */
      SELECT COUNT(*)::int AS warm_signals_fired_today
      FROM capture_inbox
      WHERE capture_type IN ('warm_signal', 'warm_signal_resolved')
        AND COALESCE(captured_at, received_at) >= $1::timestamptz - INTERVAL '24 hours'
        AND COALESCE(captured_at, received_at) < $1::timestamptz
    `),
    safeRows(`
      /* daily_health:agent_errors */
      SELECT agent_name, COUNT(*)::int AS error_count
      FROM agent_log
      WHERE status = 'error'
        AND ran_at >= $1::timestamptz - INTERVAL '24 hours'
        AND ran_at < $1::timestamptz
      GROUP BY agent_name
      ORDER BY agent_name
    `),
    safeRows(`
      /* daily_health:clients */
      WITH active_prospects AS (
        SELECT client_id, COUNT(*)::int AS active_prospect_count
        FROM prospects
        WHERE status IN ('cold', 'contacted')
        GROUP BY client_id
      ), sends AS (
        SELECT
          client_id,
          COUNT(*) FILTER (
            WHERE event_at >= $1::timestamptz - INTERVAL '24 hours'
              AND event_at < $1::timestamptz
          )::int AS send_count_today,
          COUNT(*) FILTER (
            WHERE event_at >= $1::timestamptz - INTERVAL '8 days'
              AND event_at < $1::timestamptz - INTERVAL '24 hours'
          )::int AS send_count_baseline_7d_total
        FROM email_events
        WHERE event_type IN ${SEND_EVENTS}
          AND event_at >= $1::timestamptz - INTERVAL '8 days'
          AND event_at < $1::timestamptz
        GROUP BY client_id
      )
      SELECT
        ap.client_id,
        COALESCE(c.name, c.business_name, 'Client ' || ap.client_id::text) AS client_name,
        ap.active_prospect_count,
        COALESCE(s.send_count_today, 0)::int AS send_count_today,
        COALESCE(s.send_count_baseline_7d_total, 0)::int AS send_count_baseline_7d_total
      FROM active_prospects ap
      LEFT JOIN clients c ON c.id = ap.client_id
      LEFT JOIN sends s ON s.client_id = ap.client_id
      ORDER BY ap.client_id
    `),
  ]);

  const email = emailRows[0] || {};
  const scout = scoutRows[0] || {};
  const warm = warmRows[0] || {};
  const sendCountToday = number(email.send_count_today);
  const sendBaseline = rounded(email.send_count_baseline_7d, 2);
  const bounceCount = number(email.bounce_count_today);
  const scoutToday = number(scout.scout_prospects_added_today);
  const scoutBaseline = rounded(scout.scout_baseline_7d, 2);
  const sendDelta = sendBaseline > 0
    ? rounded(((sendCountToday - sendBaseline) / sendBaseline) * 100, 2)
    : 0;
  const bounceRate = sendCountToday > 0 ? rounded(bounceCount / sendCountToday, 4) : 0;

  const agentErrors = Object.fromEntries(
    errorRows.map(row => [row.agent_name, number(row.error_count)])
  );
  const perClientSend = Object.fromEntries(
    clientRows.map(row => [String(row.client_id), number(row.send_count_today)])
  );
  const clientDetails = clientRows.map(row => ({
    client_id: number(row.client_id),
    client_name: row.client_name || `Client ${row.client_id}`,
    active_prospect_count: number(row.active_prospect_count),
    send_count_today: number(row.send_count_today),
    send_count_baseline_7d_total: number(row.send_count_baseline_7d_total),
  }));

  const flags = [];
  if (sendDelta < -50 && sendBaseline > 10) {
    flags.push({ severity: 'red', code: 'SEND_VOLUME_LOW', msg: `SEND VOLUME LOW: ${sendCountToday} sends vs 7d avg ${Math.round(sendBaseline)} (${Math.round(sendDelta)}%)` });
  }
  if (sendDelta > 200) {
    flags.push({ severity: 'yellow', code: 'SEND_VOLUME_HIGH', msg: `SEND VOLUME HIGH: ${sendCountToday} sends vs 7d avg ${Math.round(sendBaseline)} (+${Math.round(sendDelta)}%)` });
  }
  if (bounceRate > 0.04) {
    flags.push({ severity: 'red', code: 'BOUNCE_RATE_HIGH', msg: `BOUNCE RATE HIGH: ${(bounceRate * 100).toFixed(1)}% (threshold 4%)` });
  } else if (bounceRate > 0.02) {
    flags.push({ severity: 'yellow', code: 'BOUNCE_RATE_WARN', msg: `BOUNCE RATE WARN: ${(bounceRate * 100).toFixed(1)}% (threshold 2%)` });
  }
  if (scoutToday === 0 && scoutBaseline > 0) {
    flags.push({ severity: 'red', code: 'SCOUT_DRY', msg: `SCOUT DRY: 0 new prospects vs 7d avg ${Math.round(scoutBaseline)}` });
  }
  for (const [agentName, count] of Object.entries(agentErrors)) {
    if (count >= 5) flags.push({ severity: 'red', code: 'AGENT_ERRORS', msg: `AGENT ERRORS: ${agentName} logged ${count} errors`, agent_name: agentName });
  }
  for (const detail of clientDetails) {
    if (detail.active_prospect_count > 50 && detail.send_count_today === 0 && detail.send_count_baseline_7d_total > 0) {
      flags.push({
        severity: 'yellow',
        code: 'CLIENT_DARK',
        msg: `CLIENT DARK: ${detail.client_name} (${detail.client_id}) had 0 sends today`,
        client_id: detail.client_id,
      });
    }
  }
  if (!flags.length) flags.push({ severity: 'green', code: 'HEALTHY', msg: 'All systems normal' });

  return {
    as_of: asOf.toISOString(),
    send_count_today: sendCountToday,
    send_count_baseline_7d: sendBaseline,
    send_delta_pct: sendDelta,
    bounce_count_today: bounceCount,
    bounce_rate_today: bounceRate,
    reply_count_today: number(email.reply_count_today),
    scout_prospects_added_today: scoutToday,
    scout_baseline_7d: scoutBaseline,
    warm_signals_fired_today: number(warm.warm_signals_fired_today),
    agent_error_count_today: agentErrors,
    per_client_send_today: perClientSend,
    per_client_send_details: clientDetails,
    health_flags: flags,
  };
}

function formatNumber(value) {
  return Number.isInteger(number(value)) ? String(number(value)) : number(value).toFixed(1);
}

function formatDailyHealthMessage(health) {
  const date = new Date(health.as_of || Date.now()).toLocaleDateString('en-US', {
    timeZone: 'America/New_York', weekday: 'short', month: 'short', day: 'numeric',
  }).replace(',', '');
  const flags = health.health_flags || [];
  const nonGreen = flags.filter(flag => flag.severity !== 'green');
  const errors = Object.entries(health.agent_error_count_today || {}).filter(([, count]) => number(count) > 0);
  const errorTotal = errors.reduce((sum, [, count]) => sum + number(count), 0);
  const errorLabel = errors.length ? ` (${errors.map(([name]) => name).join(', ')})` : '';
  const lines = [`🩺 SYSTEM HEALTH — ${date}`, ''];

  if (nonGreen.length) {
    for (const flag of nonGreen) lines.push(`${flag.severity === 'red' ? '🚨' : '⚠️'} ${flag.msg}`);
    lines.push('');
  }

  const delta = number(health.send_delta_pct);
  const deltaText = `${delta > 0 ? '+' : ''}${Math.round(delta)}%`;
  lines.push(`Sends: ${number(health.send_count_today)} (7d avg ${formatNumber(health.send_count_baseline_7d)}, ${deltaText})`);
  lines.push(`Bounces: ${number(health.bounce_count_today)} (${(number(health.bounce_rate_today) * 100).toFixed(1)}%)`);
  lines.push(`Replies: ${number(health.reply_count_today)}`);
  lines.push(`Scout: ${number(health.scout_prospects_added_today)} new prospects (7d avg ${formatNumber(health.scout_baseline_7d)})`);
  lines.push(`Warm signals: ${number(health.warm_signals_fired_today)}`);
  lines.push(`Agent errors: ${errorTotal}${errorLabel}`);

  if (!nonGreen.length) {
    lines.push('', '🟢 All systems normal');
  } else if ((health.per_client_send_details || []).length) {
    lines.push('', 'Per-client sends today:');
    for (const detail of health.per_client_send_details) {
      const dark = flags.some(flag => flag.code === 'CLIENT_DARK' && Number(flag.client_id) === Number(detail.client_id));
      lines.push(`${detail.client_name} (${detail.client_id}): ${detail.send_count_today}${dark ? ' ⚠️' : ''}`);
    }
  }

  return lines.join('\n');
}

module.exports = { computeDailyHealth, formatDailyHealthMessage };

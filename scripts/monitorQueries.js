const pool = require('../db');

async function collectPulseHealthMetrics() {
  const [
    sendsByAction,
    failuresByVertical,
    failureSpike,
    warmProspects,
    sentTodayByClient,
    recentErrors,
  ] = await Promise.all([
    pool.query(`
      SELECT client_id, action, COUNT(*)::int AS count
      FROM agent_log
      WHERE action IN ('email_sent', 'email_failed')
        AND ran_at >= NOW() - INTERVAL '24 hours'
      GROUP BY client_id, action
      ORDER BY client_id, action
    `),
    pool.query(`
      SELECT COALESCE(payload->>'vertical', 'unknown') AS vertical, COUNT(*)::int AS count
      FROM agent_log
      WHERE action = 'email_failed'
        AND ran_at >= NOW() - INTERVAL '24 hours'
      GROUP BY COALESCE(payload->>'vertical', 'unknown')
      ORDER BY count DESC, vertical
    `),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE action = 'email_failed')::int AS email_failed_count,
        COUNT(*) FILTER (
          WHERE action = 'email_failed'
            AND (
              payload->>'error' ILIKE '%Connection timeout%'
              OR error_msg ILIKE '%Connection timeout%'
            )
        )::int AS timeout_count
      FROM agent_log
      WHERE ran_at >= NOW() - INTERVAL '2 hours'
        AND (
          action = 'email_failed'
          OR payload->>'error' ILIKE '%Connection timeout%'
          OR error_msg ILIKE '%Connection timeout%'
        )
    `),
    pool.query(`
      SELECT
        p.id,
        p.client_id,
        p.first_name,
        p.last_name,
        p.email,
        p.phone,
        p.vertical,
        p.icp_score,
        p.status,
        p.created_at,
        c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id
      WHERE p.status = 'warm'
      ORDER BY p.icp_score DESC NULLS LAST, p.created_at DESC
      LIMIT 25
    `),
    pool.query(`
      SELECT client_id, COUNT(*)::int AS count
      FROM agent_log
      WHERE action = 'email_sent'
        AND DATE(ran_at) = CURRENT_DATE
      GROUP BY client_id
      ORDER BY client_id
    `),
    pool.query(`
      SELECT id, agent_name, action, payload, status, error_msg, ran_at, client_id
      FROM agent_log
      WHERE payload ? 'error'
        AND ran_at >= NOW() - INTERVAL '24 hours'
      ORDER BY ran_at DESC
      LIMIT 15
    `),
  ]);

  return {
    sends_by_action: sendsByAction.rows,
    failures_by_vertical: failuresByVertical.rows,
    failure_spike: failureSpike.rows[0] || { email_failed_count: 0, timeout_count: 0 },
    warm_prospects: warmProspects.rows,
    sent_today_by_client: sentTodayByClient.rows,
    recent_errors: recentErrors.rows,
    checked_at: new Date().toISOString(),
  };
}

module.exports = { collectPulseHealthMetrics };

if (require.main === module) {
  collectPulseHealthMetrics()
    .then(metrics => {
      console.log(JSON.stringify(metrics, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error(err);
      await pool.end();
      process.exit(1);
    });
}

function normalizeStages(stages = []) {
  return stages
    .map(stage => ({
      afterSendDays: Math.max(0, Number(stage.afterSendDays || 0)),
      dailyCap: Math.max(1, Number(stage.dailyCap || 1)),
    }))
    .sort((a, b) => a.afterSendDays - b.afterSendDays);
}

function resolveWarmupDailyCap(stages, activeSendDays = 0) {
  const normalized = normalizeStages(stages);
  if (!normalized.length) return null;

  const sendDays = Math.max(0, Number(activeSendDays || 0));
  return normalized.reduce(
    (cap, stage) => sendDays >= stage.afterSendDays ? stage.dailyCap : cap,
    normalized[0].dailyCap
  );
}

async function getWarmupProgress(pool, clientId, resetAfterDays = 7, now = new Date()) {
  const result = await pool.query(`
    WITH ordered_sends AS (
      SELECT
        ran_at,
        LAG(ran_at) OVER (ORDER BY ran_at) AS previous_sent_at
      FROM agent_log
      WHERE action = 'email_sent'
        AND client_id = $1
    ),
    restart_points AS (
      SELECT ran_at
      FROM ordered_sends
      WHERE previous_sent_at IS NULL
         OR ran_at - previous_sent_at >= ($2::text || ' days')::interval
    ),
    latest_restart AS (
      SELECT MAX(ran_at) AS restarted_at
      FROM restart_points
    )
    SELECT
      (SELECT MAX(ran_at) FROM ordered_sends) AS last_sent_at,
      latest_restart.restarted_at,
      COUNT(DISTINCT DATE(ordered_sends.ran_at)) FILTER (
        WHERE ordered_sends.ran_at >= latest_restart.restarted_at
      )::int AS active_send_days
    FROM latest_restart
    LEFT JOIN ordered_sends ON true
    GROUP BY latest_restart.restarted_at
  `, [clientId, resetAfterDays]);

  const row = result.rows[0] || {};
  const lastSentAt = row.last_sent_at ? new Date(row.last_sent_at) : null;
  const idleDays = lastSentAt
    ? (now.getTime() - lastSentAt.getTime()) / (1000 * 60 * 60 * 24)
    : null;
  const reset = !lastSentAt || idleDays >= resetAfterDays;

  return {
    activeSendDays: reset ? 0 : Number(row.active_send_days || 0),
    idleDays,
    lastSentAt,
    restartedAt: reset || !row.restarted_at ? null : new Date(row.restarted_at),
    reset,
  };
}

module.exports = {
  getWarmupProgress,
  resolveWarmupDailyCap,
};

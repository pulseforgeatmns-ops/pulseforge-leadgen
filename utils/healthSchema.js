const pool = require('../db');

async function ensureHealthSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_health_log (
      log_date DATE PRIMARY KEY,
      ran_at TIMESTAMPTZ NOT NULL,
      send_count_today INT,
      send_count_baseline_7d NUMERIC(10,2),
      send_delta_pct NUMERIC(6,2),
      bounce_count_today INT,
      bounce_rate_today NUMERIC(5,4),
      reply_count_today INT,
      scout_prospects_added_today INT,
      scout_baseline_7d NUMERIC(10,2),
      warm_signals_fired_today INT,
      agent_error_count_today JSONB,
      per_client_send_today JSONB,
      health_flags JSONB,
      raw_snapshot JSONB
    )
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS daily_health_log_log_date_idx
      ON daily_health_log(log_date DESC)
  `);
}

async function upsertDailyHealth(health) {
  const logDate = new Date(health.as_of).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
  await pool.query(`
    INSERT INTO daily_health_log (
      log_date, ran_at, send_count_today, send_count_baseline_7d, send_delta_pct,
      bounce_count_today, bounce_rate_today, reply_count_today,
      scout_prospects_added_today, scout_baseline_7d, warm_signals_fired_today,
      agent_error_count_today, per_client_send_today, health_flags, raw_snapshot
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
      $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb
    )
    ON CONFLICT (log_date) DO UPDATE SET
      ran_at = EXCLUDED.ran_at,
      send_count_today = EXCLUDED.send_count_today,
      send_count_baseline_7d = EXCLUDED.send_count_baseline_7d,
      send_delta_pct = EXCLUDED.send_delta_pct,
      bounce_count_today = EXCLUDED.bounce_count_today,
      bounce_rate_today = EXCLUDED.bounce_rate_today,
      reply_count_today = EXCLUDED.reply_count_today,
      scout_prospects_added_today = EXCLUDED.scout_prospects_added_today,
      scout_baseline_7d = EXCLUDED.scout_baseline_7d,
      warm_signals_fired_today = EXCLUDED.warm_signals_fired_today,
      agent_error_count_today = EXCLUDED.agent_error_count_today,
      per_client_send_today = EXCLUDED.per_client_send_today,
      health_flags = EXCLUDED.health_flags,
      raw_snapshot = EXCLUDED.raw_snapshot
  `, [
    logDate, health.as_of, health.send_count_today, health.send_count_baseline_7d,
    health.send_delta_pct, health.bounce_count_today, health.bounce_rate_today,
    health.reply_count_today, health.scout_prospects_added_today, health.scout_baseline_7d,
    health.warm_signals_fired_today, JSON.stringify(health.agent_error_count_today || {}),
    JSON.stringify(health.per_client_send_today || {}), JSON.stringify(health.health_flags || []),
    JSON.stringify(health),
  ]);
}

module.exports = { ensureHealthSchema, upsertDailyHealth };

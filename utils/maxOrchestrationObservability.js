const pool = require('../db');

const METRICS = new Set([
  'max_decisions_total',
  'max_state_transition_recommendations_total',
  'max_action_recommendations_total',
  'max_evaluation_failures_total',
  'max_duplicate_events_suppressed_total',
  'max_decay_evaluations_total',
  'max_manual_overrides_total',
  'signal_to_decision_duration',
  'decision_processing_duration',
  'decay_batch_duration',
  'live_signal_to_decision_latency',
  'historical_backfill_event_age',
  'historical_backfill_processing_latency',
  'manual_recalculation_processing_latency',
  'decay_processing_latency',
]);

async function recordMaxMetric(metricName, {
  db = pool,
  clientId = null,
  value = 1,
  prospectId = null,
  signalEventId = null,
  decisionId = null,
  dimensions = {},
} = {}) {
  if (!METRICS.has(metricName)) throw new Error(`Unknown Max orchestration metric: ${metricName}`);
  await db.query(`
    INSERT INTO max_orchestration_metrics
      (client_id, metric_name, metric_value, prospect_id, signal_event_id, decision_id, dimensions)
    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)
  `, [clientId, metricName, value, prospectId, signalEventId, decisionId, JSON.stringify(dimensions || {})]);
}

async function logMaxOrchestrationFailure({
  db = pool,
  clientId = null,
  prospectId = null,
  action = 'evaluation_failed',
  error,
  payload = {},
}) {
  const message = String(error?.message || error || 'Unknown orchestration failure').slice(0, 1000);
  await Promise.allSettled([
    recordMaxMetric('max_evaluation_failures_total', {
      db, clientId, prospectId, dimensions: { action, error: message },
    }),
    db.query(`
      INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, error_msg, ran_at, client_id)
      VALUES ('max_orchestration',$1,$2,$3::jsonb,'failed',$4,NOW(),$5)
    `, [action, prospectId, JSON.stringify({ ...payload, error: message }), message, clientId]),
  ]);
}

module.exports = { METRICS, logMaxOrchestrationFailure, recordMaxMetric };

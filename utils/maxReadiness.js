'use strict';

const pool = require('../db');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { validateSchema } = require('../scripts/validateMaxOrchestrationSchema');

const available = value => ({ status: 'available', value });
const unavailable = reason => ({ status: 'unavailable', value: null, reason });

async function safeQuery(db, sql, params, mapper = row => row) {
  try {
    const result = await db.query(sql, params);
    return available(mapper(result.rows[0] || {}, result.rows));
  } catch (error) {
    if (['42P01','42703'].includes(error.code)) return unavailable(error.message);
    throw error;
  }
}

function rate(numerator, denominator) {
  return denominator > 0 ? Number((100 * numerator / denominator).toFixed(2)) : null;
}

async function buildReadinessReport({ clientId, sinceDays = 30 } = {}, db = pool) {
  const params = [clientId, sinceDays];
  const [signals, failures, evaluationFailures, duplicates, proxyViolations, decisions, transitions, actions, latency,
    oscillation, reviews, warmNoChannel, terminalAccuracy, outcomes, reviewedByClient, sourcePresence] = await Promise.all([
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospect_signal_events WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM agent_log WHERE ($1::int IS NULL OR client_id=$1) AND agent_name='max_orchestration' AND action='signal_ingestion_failed' AND ran_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COALESCE(SUM(metric_value),0)::int count FROM max_orchestration_metrics WHERE ($1::int IS NULL OR client_id=$1) AND metric_name='max_evaluation_failures_total' AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COALESCE(SUM(metric_value),0)::int count FROM max_orchestration_metrics WHERE ($1::int IS NULL OR client_id=$1) AND metric_name='max_duplicate_events_suppressed_total' AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospect_signal_events s LEFT JOIN email_events e ON s.source='brevo' AND e.event_id=s.source_record_id WHERE ($1::int IS NULL OR s.client_id=$1) AND s.event_type='email_human_opened' AND s.created_at>=NOW()-($2::int*INTERVAL '1 day') AND (COALESCE(s.metadata->>'open_source','unknown')<>'human' OR (e.event_id IS NOT NULL AND e.open_source::text<>'human'))`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM max_decisions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT current_state||' -> '||recommended_state transition,COUNT(*)::int count FROM max_decisions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY 1 ORDER BY count DESC`, params, (_row, rows) => rows),
    safeQuery(db, `SELECT COUNT(*) FILTER (WHERE action_status<>'skipped')::int executed,COUNT(*) FILTER (WHERE action_status='skipped' AND error_code='SHADOW_MODE')::int shadow_skipped FROM max_actions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => ({ executed:Number(row.executed||0), shadow_skipped:Number(row.shadow_skipped||0) })),
    safeQuery(db, `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value)::numeric median_ms,percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value)::numeric p95_ms FROM max_orchestration_metrics WHERE ($1::int IS NULL OR client_id=$1) AND metric_name='signal_to_decision_duration' AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => row.median_ms == null ? null : ({ median_ms:Number(row.median_ms), p95_ms:Number(row.p95_ms) })),
    safeQuery(db, `WITH ordered AS (SELECT prospect_id,from_state,to_state,created_at,LAG(from_state) OVER(PARTITION BY prospect_id ORDER BY created_at) prev_from,LAG(to_state) OVER(PARTITION BY prospect_id ORDER BY created_at) prev_to FROM prospect_state_transitions WHERE ($1::int IS NULL OR client_id=$1) AND is_shadow=true AND created_at>=NOW()-($2::int*INTERVAL '1 day')) SELECT COUNT(*) FILTER(WHERE from_state=prev_to AND to_state=prev_from)::int reversals,COUNT(*)::int transitions FROM ordered`, params, row => ({ reversals:Number(row.reversals||0), transitions:Number(row.transitions||0), rate_pct:rate(Number(row.reversals||0),Number(row.transitions||0)) })),
    safeQuery(db, `SELECT COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE review_outcome='agree')::int agree,COUNT(*) FILTER(WHERE review_outcome='disagree')::int disagree FROM max_recommendation_reviews WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => ({ reviewed:Number(row.reviewed||0), agree:Number(row.agree||0), disagree:Number(row.disagree||0), agreement_rate_pct:rate(Number(row.agree||0),Number(row.reviewed||0)) })),
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospects WHERE ($1::int IS NULL OR client_id=$1) AND lifecycle_state IN ('warm','hot','engaged') AND COALESCE(do_not_contact,false)=false AND NULLIF(email,'') IS NULL AND NULLIF(phone,'') IS NULL`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE r.review_outcome='agree')::int accurate FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id WHERE ($1::int IS NULL OR r.client_id=$1) AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day') AND d.recommended_state IN ('disqualified','null')`, params, row => ({ reviewed:Number(row.reviewed||0), accurate:Number(row.accurate||0), accuracy_rate_pct:rate(Number(row.accurate||0),Number(row.reviewed||0)) })),
    safeQuery(db, `SELECT COUNT(*)::int warm_recommendations,COUNT(*) FILTER(WHERE EXISTS(SELECT 1 FROM prospect_signal_events s WHERE s.client_id=d.client_id AND s.prospect_id=d.prospect_id AND s.event_timestamp>d.created_at AND s.event_type IN ('email_positive_reply','meeting_booked','meeting_showed')))::int positive_outcomes FROM max_decisions d WHERE ($1::int IS NULL OR d.client_id=$1) AND d.recommended_state IN ('warm','hot') AND d.created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => ({ warm_recommendations:Number(row.warm_recommendations||0), positive_outcomes:Number(row.positive_outcomes||0), outcome_rate_pct:rate(Number(row.positive_outcomes||0),Number(row.warm_recommendations||0)) })),
    safeQuery(db, `SELECT client_id,COUNT(*)::int reviewed FROM max_recommendation_reviews WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY client_id ORDER BY client_id`, params, (_row, rows) => rows),
    safeQuery(db, `SELECT to_regclass('public.email_events') IS NOT NULL email_events,to_regclass('public.icp_score_history') IS NOT NULL icp_history,to_regclass('public.touchpoints') IS NOT NULL replies,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='prospect_discovered') scout_discovery,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='prospect_qualified') scout_qualification,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_booked') meeting_booked,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_cancelled') meeting_cancelled,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_showed') meeting_showed,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_no_showed') meeting_no_showed`, [], row => row),
  ]);
  const signalCount = signals.value || 0;
  const failureCount = failures.value || 0;
  const duplicateCount = duplicates.value || 0;
  const source = sourcePresence.value || {};
  const missingSources = sourcePresence.status === 'available'
    ? Object.entries(source).filter(([,present]) => !present).map(([name]) => name)
    : null;
  return {
    generated_at: new Date().toISOString(), client_id: clientId ?? null, window_days: sinceDays,
    signals_processed: signals,
    signal_ingestion_failures: failures,
    signal_ingestion_success_rate_pct: signals.status==='available'&&failures.status==='available' ? available(rate(signalCount,signalCount+failureCount)) : unavailable('signal or failure source unavailable'),
    duplicate_suppression: duplicates,
    duplicate_suppression_rate_pct: signals.status==='available'&&duplicates.status==='available' ? available(rate(duplicateCount,signalCount+duplicateCount)) : unavailable('signal or duplicate source unavailable'),
    proxy_open_scoring_violations: proxyViolations,
    decisions_generated: decisions,
    decisions_by_transition: transitions,
    prospect_facing_actions: actions,
    decision_processing_failures: evaluationFailures,
    signal_to_decision_latency: latency,
    oscillation: oscillation,
    manual_review: reviews,
    agreement_by_transition: await safeQuery(db, `SELECT d.current_state||' -> '||d.recommended_state transition,COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE r.review_outcome='agree')::int agree FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id WHERE ($1::int IS NULL OR r.client_id=$1) AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY 1 ORDER BY reviewed DESC`, params, (_row,rows)=>rows.map(row=>({...row,agreement_rate_pct:rate(Number(row.agree),Number(row.reviewed))}))),
    warm_without_reachable_channels: warmNoChannel,
    terminal_recommendation_accuracy: terminalAccuracy,
    historical_warm_outcomes: outcomes,
    missing_canonical_sources: sourcePresence.status==='available' ? available(missingSources) : sourcePresence,
    reviewed_recommendations_by_client: reviewedByClient,
  };
}

async function checkPhase3Readiness({ clientId, sinceDays = 30, env = process.env } = {}, db = pool) {
  if (!Number.isInteger(clientId) || clientId < 1) throw new Error('A positive target client ID is required');
  const [schema, report, configRow, clientRow] = await Promise.all([
    validateSchema(db),
    buildReadinessReport({ clientId, sinceDays }, db),
    db.query('SELECT * FROM max_rollout_readiness_config WHERE client_id=$1', [clientId]),
    db.query('SELECT max_orchestration_config FROM clients WHERE id=$1 AND active=true', [clientId]),
  ]);
  const client = clientRow.rows[0];
  const rollout = configRow.rows[0];
  const config = client ? loadMaxOrchestrationConfig({ env, clientOverrides: client.max_orchestration_config }) : null;
  const actionFlags = config ? {
    max_state_transitions_enabled: config.flags.max_state_transitions_enabled,
    max_enrichment_actions_enabled: config.flags.max_enrichment_actions_enabled,
    max_warm_sequence_enabled: config.flags.max_warm_sequence_enabled,
    max_call_tasks_enabled: config.flags.max_call_tasks_enabled,
    max_hot_escalations_enabled: config.flags.max_hot_escalations_enabled,
    max_recycle_actions_enabled: config.flags.max_recycle_actions_enabled,
    max_sequence_actions_enabled: config.flags.max_sequence_actions_enabled,
    max_operator_tasks_enabled: config.flags.max_operator_tasks_enabled,
    max_enrichment_retry_enabled: config.flags.max_enrichment_retry_enabled,
    max_prospect_actions_enabled: config.flags.max_prospect_actions_enabled,
  } : {};
  const reviews = report.manual_review.value?.reviewed ?? 0;
  const criteria = {
    schema_validation_passes: schema.valid,
    active_client_exists: Boolean(client),
    shadow_mode_enabled: Boolean(config?.flags.max_shadow_mode),
    orchestration_enabled: Boolean(config?.enabled && config?.flags.max_scoring_enabled),
    prospect_facing_flags_disabled: config ? Object.values(actionFlags).every(value=>value===false) : false,
    no_proxy_open_scoring_violations: report.proxy_open_scoring_violations.status==='available' && report.proxy_open_scoring_violations.value===0,
    no_prospect_facing_actions_executed: report.prospect_facing_actions.status==='available' && report.prospect_facing_actions.value.executed===0,
    minimum_reviewed_samples_met: Boolean(rollout && reviews>=Number(rollout.minimum_reviewed_samples)),
    rollback_configuration_exists: Boolean(rollout?.rollback_documented && rollout?.rollback_reference),
    target_client_allowlisted: rollout?.phase3_allowlisted===true,
  };
  return { ready: Object.values(criteria).every(Boolean), client_id: clientId, criteria, action_flags: actionFlags, review_count: reviews, required_review_count: rollout?.minimum_reviewed_samples ?? null, report };
}

module.exports = { available, buildReadinessReport, checkPhase3Readiness, rate, safeQuery, unavailable };

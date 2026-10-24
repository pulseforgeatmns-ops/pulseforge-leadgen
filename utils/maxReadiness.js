'use strict';

const pool = require('../db');
const { loadMaxOrchestrationConfig } = require('../config/maxOrchestration');
const { validateSchema } = require('../scripts/validateMaxOrchestrationSchema');
const { buildEventCoverageReport } = require('./maxEventCoverage');
const { buildReviewConsistencyReport } = require('./maxReviewConsistency');
const { buildWarmOutcomeValidation } = require('./maxOutcomeValidation');

const MIN_LIVE_LATENCY_SAMPLES = 5;

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

function uniqueReviewCount(report) {
  return Number(report?.review_consistency?.value?.unique_decisions_reviewed || 0);
}

async function latencyPercentiles(db, metricName, params, unavailableReason, { minimumSamples = 1 } = {}) {
  const result = await safeQuery(db, `
    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY metric_value)::numeric median_ms,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY metric_value)::numeric p95_ms,
           COUNT(*)::int samples
    FROM max_orchestration_metrics
    WHERE ($1::int IS NULL OR client_id=$1) AND metric_name=$3
      AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')
  `, [...params, metricName], row => row.median_ms == null ? null : ({
    median_ms: Number(row.median_ms), p95_ms: Number(row.p95_ms), samples: Number(row.samples || 0),
  }));
  if (result.status !== 'available') return result;
  const samples = Number(result.value?.samples || 0);
  if (result.value == null || samples < minimumSamples) {
    return {
      ...unavailable(minimumSamples > 1
        ? `${unavailableReason}; requires at least ${minimumSamples} qualifying sample(s)`
        : unavailableReason),
      samples,
      minimum_samples: minimumSamples,
    };
  }
  return result;
}

async function liveLatencyBreakdown(db, params) {
  return safeQuery(db, `
    SELECT client_id,dimensions->>'event_type' event_type,
           COUNT(*)::int samples,
           percentile_cont(0.5) WITHIN GROUP(ORDER BY metric_value)::numeric median_ms,
           percentile_cont(0.95) WITHIN GROUP(ORDER BY metric_value)::numeric p95_ms
    FROM max_orchestration_metrics
    WHERE ($1::int IS NULL OR client_id=$1) AND metric_name=$3
      AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')
    GROUP BY client_id,dimensions->>'event_type' ORDER BY client_id,event_type
  `,[...params,'live_signal_to_decision_latency'],(_row,rows)=>rows.map(row=>({
    client_id:row.client_id,event_type:row.event_type,samples:Number(row.samples||0),
    median_ms:Number(row.median_ms),p95_ms:Number(row.p95_ms),
  })));
}

async function buildReadinessReport({ clientId, sinceDays = 30 } = {}, db = pool) {
  const params = [clientId, sinceDays];
  const [signals, failures, evaluationFailures, duplicates, proxyViolations, decisions, transitions, actions,
    liveLatency, liveProcessing, liveBreakdown, historicalAge, historicalProcessing, manualProcessing, decayProcessing,
    oscillation, reviews, reviewConsistency, warmNoChannel, terminalAccuracy, terminalCoverage,
    outcomes, reviewedByClient, sourcePresence, coverage] = await Promise.all([
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospect_signal_events WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM agent_log WHERE ($1::int IS NULL OR client_id=$1) AND agent_name='max_orchestration' AND action='signal_ingestion_failed' AND ran_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COALESCE(SUM(metric_value),0)::int count FROM max_orchestration_metrics WHERE ($1::int IS NULL OR client_id=$1) AND metric_name='max_evaluation_failures_total' AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COALESCE(SUM(metric_value),0)::int count FROM max_orchestration_metrics WHERE ($1::int IS NULL OR client_id=$1) AND metric_name='max_duplicate_events_suppressed_total' AND recorded_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospect_signal_events s LEFT JOIN email_events e ON s.source='brevo' AND e.event_id=s.source_record_id WHERE ($1::int IS NULL OR s.client_id=$1) AND s.event_type='email_human_opened' AND s.created_at>=NOW()-($2::int*INTERVAL '1 day') AND (COALESCE(s.metadata->>'open_source','unknown')<>'human' OR (e.event_id IS NOT NULL AND e.open_source::text<>'human'))`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int count FROM max_decisions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => Number(row.count||0)),
    safeQuery(db, `SELECT current_state||' -> '||recommended_state transition,COUNT(*)::int count FROM max_decisions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY 1 ORDER BY count DESC`, params, (_row, rows) => rows),
    safeQuery(db, `SELECT COUNT(*) FILTER (WHERE action_status<>'skipped')::int executed,COUNT(*) FILTER (WHERE action_status='skipped' AND error_code='SHADOW_MODE')::int shadow_skipped FROM max_actions WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => ({ executed:Number(row.executed||0), shadow_skipped:Number(row.shadow_skipped||0) })),
    latencyPercentiles(db, 'live_signal_to_decision_latency', params, 'insufficient qualifying live decisions in the reporting window', { minimumSamples: MIN_LIVE_LATENCY_SAMPLES }),
    latencyPercentiles(db, 'live_processing_latency', params, 'insufficient instrumented live processing samples in the reporting window', { minimumSamples: MIN_LIVE_LATENCY_SAMPLES }),
    liveLatencyBreakdown(db, params),
    safeQuery(db, `
      SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY GREATEST(0,EXTRACT(EPOCH FROM (created_at-event_timestamp))*1000))::numeric median_ms,
             percentile_cont(0.95) WITHIN GROUP (ORDER BY GREATEST(0,EXTRACT(EPOCH FROM (created_at-event_timestamp))*1000))::numeric p95_ms,
             COUNT(*)::int samples
      FROM prospect_signal_events
      WHERE ($1::int IS NULL OR client_id=$1) AND created_at>=NOW()-($2::int*INTERVAL '1 day')
        AND (metadata->>'provenance'='historical_backfill' OR metadata->>'historical_backfill'='true')
    `, params, row => row.median_ms == null ? null : ({ median_ms:Number(row.median_ms), p95_ms:Number(row.p95_ms), samples:Number(row.samples||0) })),
    latencyPercentiles(db, 'historical_backfill_processing_latency', params, 'no instrumented historical backfill decisions in the reporting window'),
    latencyPercentiles(db, 'manual_recalculation_processing_latency', params, 'no manual recalculation decisions in the reporting window'),
    latencyPercentiles(db, 'decay_processing_latency', params, 'no applied decay decisions in the reporting window'),
    safeQuery(db, `WITH ordered AS (SELECT prospect_id,from_state,to_state,created_at,LAG(from_state) OVER(PARTITION BY prospect_id ORDER BY created_at) prev_from,LAG(to_state) OVER(PARTITION BY prospect_id ORDER BY created_at) prev_to FROM prospect_state_transitions WHERE ($1::int IS NULL OR client_id=$1) AND is_shadow=true AND created_at>=NOW()-($2::int*INTERVAL '1 day')) SELECT COUNT(*) FILTER(WHERE from_state=prev_to AND to_state=prev_from)::int reversals,COUNT(*)::int transitions FROM ordered`, params, row => ({ reversals:Number(row.reversals||0), transitions:Number(row.transitions||0), rate_pct:rate(Number(row.reversals||0),Number(row.transitions||0)) })),
    safeQuery(db, `SELECT COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE review_outcome='agree')::int agree,COUNT(*) FILTER(WHERE review_outcome='disagree')::int disagree FROM max_recommendation_reviews WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day')`, params, row => ({ reviewed:Number(row.reviewed||0), agree:Number(row.agree||0), disagree:Number(row.disagree||0), agreement_rate_pct:rate(Number(row.agree||0),Number(row.reviewed||0)) })),
    buildReviewConsistencyReport({ clientId, sinceDays }, db),
    safeQuery(db, `SELECT COUNT(*)::int count FROM prospects WHERE ($1::int IS NULL OR client_id=$1) AND lifecycle_state IN ('warm','hot','engaged') AND COALESCE(do_not_contact,false)=false AND NULLIF(email,'') IS NULL AND NULLIF(phone,'') IS NULL`, [clientId], row => Number(row.count||0)),
    safeQuery(db, `SELECT COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE r.review_outcome='agree')::int accurate FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id WHERE ($1::int IS NULL OR r.client_id=$1) AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day') AND d.recommended_state IN ('disqualified','null')`, params, row => ({ reviewed:Number(row.reviewed||0), accurate:Number(row.accurate||0), accuracy_rate_pct:rate(Number(row.accurate||0),Number(row.reviewed||0)) })),
    safeQuery(db, `SELECT COUNT(*)::int terminal_decisions,COUNT(*) FILTER(WHERE EXISTS(SELECT 1 FROM max_recommendation_reviews r WHERE r.decision_id=d.id))::int terminal_reviewed FROM max_decisions d WHERE ($1::int IS NULL OR d.client_id=$1) AND d.created_at>=NOW()-($2::int*INTERVAL '1 day') AND d.recommended_state IN ('disqualified','null')`, params, row => ({ terminal_decisions:Number(row.terminal_decisions||0), terminal_reviewed:Number(row.terminal_reviewed||0) })),
    buildWarmOutcomeValidation({ clientId, sinceDays: Math.max(sinceDays,30) }, db),
    safeQuery(db, `SELECT client_id,COUNT(*)::int reviewed FROM max_recommendation_reviews WHERE ($1::int IS NULL OR client_id=$1) AND reviewed_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY client_id ORDER BY client_id`, params, (_row, rows) => rows),
    safeQuery(db, `SELECT to_regclass('public.email_events') IS NOT NULL email_events,to_regclass('public.icp_score_history') IS NOT NULL icp_history,to_regclass('public.touchpoints') IS NOT NULL replies,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='prospect_discovered') scout_discovery,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='prospect_qualified') scout_qualification,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_booked') meeting_booked,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_cancelled') meeting_cancelled,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_showed') meeting_showed,EXISTS(SELECT 1 FROM prospect_signal_events WHERE event_type='meeting_no_showed') meeting_no_showed`, [], row => row),
    buildEventCoverageReport({ clientId }, db),
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
    live_signal_to_decision_latency: liveLatency,
    live_processing_latency: liveProcessing,
    live_latency_minimum_samples: MIN_LIVE_LATENCY_SAMPLES,
    live_latency_by_event_type_and_client: liveBreakdown,
    historical_backfill_event_age: historicalAge.status === 'available' && historicalAge.value == null
      ? unavailable('no historical backfill signals in the reporting window') : historicalAge,
    historical_backfill_processing_latency: historicalProcessing,
    manual_recalculation_processing_latency: manualProcessing,
    decay_processing_latency: decayProcessing,
    oscillation: oscillation,
    manual_review: reviews,
    review_consistency: available(reviewConsistency),
    agreement_by_transition: await safeQuery(db, `SELECT d.current_state||' -> '||d.recommended_state transition,COUNT(*)::int reviewed,COUNT(*) FILTER(WHERE r.review_outcome='agree')::int agree FROM max_recommendation_reviews r JOIN max_decisions d ON d.id=r.decision_id WHERE ($1::int IS NULL OR r.client_id=$1) AND r.reviewed_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY 1 ORDER BY reviewed DESC`, params, (_row,rows)=>rows.map(row=>({...row,agreement_rate_pct:rate(Number(row.agree),Number(row.reviewed))}))),
    warm_without_reachable_channels: warmNoChannel,
    terminal_recommendation_accuracy: terminalAccuracy,
    terminal_review_coverage: terminalCoverage,
    warm_recommendation_outcome_validation: available(outcomes),
    missing_canonical_sources: sourcePresence.status==='available' ? available(missingSources) : sourcePresence,
    reviewed_recommendations_by_client: reviewedByClient,
    event_coverage: available(coverage.rows),
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
  const reviews = uniqueReviewCount(report);
  const requiredReviews = rollout?.minimum_total_reviews ?? rollout?.minimum_reviewed_samples ?? null;
  const terminal = report.terminal_review_coverage.value || {};
  const criteria = {
    schema_validation_passes: schema.valid,
    active_client_exists: Boolean(client),
    shadow_mode_enabled: Boolean(config?.flags.max_shadow_mode),
    orchestration_enabled: Boolean(config?.enabled && config?.flags.max_scoring_enabled),
    prospect_facing_flags_disabled: config ? Object.values(actionFlags).every(value=>value===false) : false,
    no_proxy_open_scoring_violations: report.proxy_open_scoring_violations.status==='available' && report.proxy_open_scoring_violations.value===0,
    no_prospect_facing_actions_executed: report.prospect_facing_actions.status==='available' && report.prospect_facing_actions.value.executed===0,
    shadow_observation_configured: rollout?.shadow_observation_enabled === true,
    minimum_reviewed_samples_met: Boolean(rollout && requiredReviews && reviews>=Number(requiredReviews)),
    terminal_review_requirement_met: Boolean(rollout && (
      rollout.terminal_review_requirement !== 'every'
      || Number(terminal.terminal_reviewed || 0) === Number(terminal.terminal_decisions || 0)
    )),
    rollback_reference_recorded: Boolean(rollout?.rollback_documented && rollout?.rollback_reference),
    rollback_reference_verified: rollout?.rollback_reference_verified === true,
    database_recovery_reference_recorded: Boolean(rollout?.recovery_snapshot_reference),
    recovery_artifact_found: rollout?.recovery_artifact_found === true,
    recovery_hash_verified: rollout?.recovery_hash_verified === true,
    recovery_archive_readable: rollout?.recovery_archive_readable === true,
    recovery_restore_procedure_documented: rollout?.recovery_restore_procedure_documented === true,
    recovery_durable_storage_verified: rollout?.recovery_durable_storage_verified === true,
    database_recovery_reference_verified: rollout?.recovery_snapshot_verified === true,
    decay_schedule_configured: rollout?.decay_schedule_configured === true,
    decay_schedule_verified: rollout?.decay_schedule_verified === true,
    target_client_allowlisted: rollout?.phase3_allowlisted===true,
  };
  return {
    ready: Object.values(criteria).every(Boolean), client_id: clientId, criteria, action_flags: actionFlags,
    review_count: reviews, required_review_count: requiredReviews,
    readiness_config: rollout || null, report,
  };
}

module.exports = {
  MIN_LIVE_LATENCY_SAMPLES, available, buildReadinessReport, checkPhase3Readiness,
  latencyPercentiles, liveLatencyBreakdown, rate, safeQuery, unavailable, uniqueReviewCount,
};

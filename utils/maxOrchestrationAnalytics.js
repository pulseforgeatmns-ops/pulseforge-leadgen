const pool = require('../db');

const LIFECYCLE_LABELS = Object.freeze({
  'cold>heating': 'Cold → Heating',
  'heating>warm': 'Heating → Warm',
  'warm>hot': 'Warm → Hot',
  engaged: 'Any → Engaged',
  recycle: 'Warm/Hot → Recycle',
  disqualified: 'Any → Disqualified',
  null: 'Any → Null',
});

function number(value) {
  return Number(value || 0);
}

function stageConversion(entering, advancing, minSample) {
  if (entering == null || advancing == null || entering === 0) return { available: false, reason: 'canonical source unavailable' };
  if (entering < minSample) return { available: false, reason: `sample below ${minSample}`, entering, advancing };
  return { available: true, entering, advancing, conversion_rate: Number(((advancing / entering) * 100).toFixed(1)) };
}

async function getShadowDigestData({ db = pool, clientId, hours = 24, minSample = 20 } = {}) {
  const [movement, latest, activity, funnelPairs, opens] = await Promise.all([
    db.query(`
      SELECT from_state, to_state, COUNT(*)::int AS count
      FROM prospect_state_transitions
      WHERE client_id = $1 AND is_shadow = TRUE AND applied = FALSE
        AND created_at >= NOW() - ($2::int * INTERVAL '1 hour')
      GROUP BY from_state, to_state
    `, [clientId, hours]),
    db.query(`
      WITH latest AS (
        SELECT DISTINCT ON (d.prospect_id)
          d.prospect_id, d.recommended_state, d.next_best_action, d.created_at,
          p.email, p.email_verified, p.phone, p.active_sequence_type, p.active_sequence_id
        FROM max_decisions d
        JOIN prospects p ON p.id = d.prospect_id AND p.client_id = d.client_id
        WHERE d.client_id = $1 AND d.is_shadow = TRUE
        ORDER BY d.prospect_id, d.created_at DESC
      )
      SELECT
        COUNT(*) FILTER (WHERE recommended_state = 'warm')::int AS total_warm,
        COUNT(*) FILTER (WHERE recommended_state = 'warm' AND created_at >= NOW() - ($2::int * INTERVAL '1 hour'))::int AS new_warm,
        COUNT(*) FILTER (WHERE recommended_state = 'warm' AND (email IS NULL OR email_verified IS DISTINCT FROM TRUE))::int AS without_verified_email,
        COUNT(*) FILTER (WHERE recommended_state = 'warm' AND phone IS NOT NULL AND BTRIM(phone) <> '')::int AS with_phone,
        COUNT(*) FILTER (WHERE recommended_state = 'warm' AND next_best_action IN ('prioritized_enrichment','operator_review'))::int AS blocked,
        COUNT(*) FILTER (WHERE recommended_state = 'warm' AND COALESCE(active_sequence_type, 'none') = 'none')::int AS missing_sequence_config
      FROM latest
    `, [clientId, hours]),
    db.query(`
      SELECT
        (SELECT COUNT(*) FROM max_decisions WHERE client_id=$1 AND created_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS decisions,
        (SELECT COUNT(*) FROM max_actions WHERE client_id=$1 AND created_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS actions,
        (SELECT COALESCE(SUM(metric_value),0) FROM max_orchestration_metrics WHERE client_id=$1 AND metric_name='max_duplicate_events_suppressed_total' AND recorded_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS duplicates,
        (SELECT COALESCE(SUM(metric_value),0) FROM max_orchestration_metrics WHERE client_id=$1 AND metric_name='max_evaluation_failures_total' AND recorded_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS failures,
        (SELECT COALESCE(SUM(metric_value),0) FROM max_orchestration_metrics WHERE client_id=$1 AND metric_name='max_decay_evaluations_total' AND recorded_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS decay_evaluations,
        (SELECT COUNT(*) FROM manual_lifecycle_overrides WHERE client_id=$1 AND created_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS manual_overrides,
        (SELECT COUNT(*) FROM max_decisions WHERE client_id=$1 AND operator_required=TRUE AND created_at >= NOW()-($2::int*INTERVAL '1 hour'))::int AS requiring_review
    `, [clientId, hours]),
    db.query(`
      WITH per_prospect AS (
        SELECT prospect_id,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='prospect_discovered') AS discovered,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='prospect_qualified') AS qualified,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='enrichment_succeeded') AS enriched,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='email_verified') AS verified,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='sequence_started') AS sequenced,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='email_delivered') AS delivered,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='email_human_opened') AS human_opened,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type IN ('email_replied','email_meaningful_reply','email_positive_reply','email_negative_reply')) AS replied,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='email_positive_reply') AS positive_reply,
          MIN(event.event_timestamp) FILTER (WHERE event.event_type='meeting_booked') AS meeting_booked
        FROM prospect_signal_events event
        LEFT JOIN email_events raw_email
          ON event.source='brevo' AND raw_email.event_id=event.source_record_id
        WHERE event.client_id=$1 AND event.event_timestamp>=NOW()-($2::int*INTERVAL '1 hour')
          AND NOT (event.event_type='email_human_opened' AND event.source='brevo' AND COALESCE(raw_email.open_source::text,'unknown')<>'human')
        GROUP BY event.prospect_id
      ), pairs AS (
        SELECT 'Discovered → Qualified' AS label, discovered AS entered_at, qualified AS advanced_at FROM per_prospect
        UNION ALL SELECT 'Qualified → Enriched',qualified,enriched FROM per_prospect
        UNION ALL SELECT 'Enriched → Verified',enriched,verified FROM per_prospect
        UNION ALL SELECT 'Verified → Sequenced',verified,sequenced FROM per_prospect
        UNION ALL SELECT 'Sequenced → Delivered',sequenced,delivered FROM per_prospect
        UNION ALL SELECT 'Delivered → Human opened',delivered,human_opened FROM per_prospect
        UNION ALL SELECT 'Human opened → Replied',human_opened,replied FROM per_prospect
        UNION ALL SELECT 'Replied → Positive reply',replied,positive_reply FROM per_prospect
        UNION ALL SELECT 'Positive reply → Meeting booked',positive_reply,meeting_booked FROM per_prospect
      )
      SELECT label,
        COUNT(*) FILTER (WHERE entered_at IS NOT NULL)::int AS entering,
        COUNT(*) FILTER (WHERE entered_at IS NOT NULL AND advanced_at >= entered_at)::int AS advancing
      FROM pairs GROUP BY label
    `, [clientId, hours]),
    db.query(`
      SELECT
        COUNT(DISTINCT event.prospect_id) FILTER (WHERE event.event_type='email_human_opened')::int AS human,
        COUNT(DISTINCT event.prospect_id) FILTER (WHERE event.event_type='email_proxy_opened')::int AS proxy,
        COUNT(DISTINCT event.prospect_id) FILTER (WHERE event.event_type='email_unknown_opened')::int AS unknown
      FROM prospect_signal_events event
      LEFT JOIN email_events raw_email
        ON event.source='brevo' AND raw_email.event_id=event.source_record_id
      WHERE event.client_id=$1 AND event.event_timestamp>=NOW()-($2::int*INTERVAL '1 hour')
        AND NOT (event.event_type='email_human_opened' AND event.source='brevo' AND COALESCE(raw_email.open_source::text,'unknown')<>'human')
    `, [clientId, hours]),
  ]);
  const movementMap = {};
  for (const row of movement.rows) {
    const key = `${row.from_state}>${row.to_state}`;
    if (LIFECYCLE_LABELS[key]) movementMap[LIFECYCLE_LABELS[key]] = number(row.count);
    if (LIFECYCLE_LABELS[row.to_state]) movementMap[LIFECYCLE_LABELS[row.to_state]] = (movementMap[LIFECYCLE_LABELS[row.to_state]] || 0) + number(row.count);
    if (row.to_state === 'recycle' && ['warm', 'hot'].includes(row.from_state)) movementMap[LIFECYCLE_LABELS.recycle] = (movementMap[LIFECYCLE_LABELS.recycle] || 0) + number(row.count);
  }
  return {
    hours,
    min_sample: minSample,
    lifecycle_recommendations: movementMap,
    warm_queue: latest.rows[0] || {},
    activity: activity.rows[0] || {},
    open_breakdown: opens.rows[0] || { human: 0, proxy: 0, unknown: 0 },
    funnel: funnelPairs.rows.map(row => ({ label: row.label, ...stageConversion(number(row.entering), number(row.advancing), minSample) })),
  };
}

function formatShadowDigest(data) {
  if (!data) return '';
  const lines = ['MAX ORCHESTRATION — SHADOW ONLY', 'Recommendations below were not executed.'];
  const moves = Object.entries(data.lifecycle_recommendations || {});
  lines.push('Lifecycle recommendations:');
  lines.push(moves.length ? moves.map(([label, count]) => `  ${label}: ${count}`).join('\n') : '  None in period.');
  const warm = data.warm_queue || {};
  lines.push(`Warm queue: ${number(warm.total_warm)} total; ${number(warm.new_warm)} new; ${number(warm.without_verified_email)} without verified email; ${number(warm.with_phone)} with phone; ${number(warm.blocked)} blocked; ${number(warm.missing_sequence_config)} missing sequence configuration.`);
  const activity = data.activity || {};
  lines.push(`Max activity: ${number(activity.decisions)} decisions; ${number(activity.actions)} skipped action recommendations; ${number(activity.duplicates)} duplicates suppressed; ${number(activity.failures)} failures; ${number(activity.decay_evaluations)} decay evaluations; ${number(activity.manual_overrides)} manual overrides; ${number(activity.requiring_review)} requiring review.`);
  const opens = data.open_breakdown || {};
  lines.push(`Open signals: ${number(opens.human)} human; ${number(opens.proxy)} proxy; ${number(opens.unknown)} unknown.`);
  lines.push('Funnel leakage:');
  for (const stage of data.funnel || []) {
    lines.push(stage.available
      ? `  ${stage.label}: ${stage.advancing}/${stage.entering} (${stage.conversion_rate}%)`
      : `  ${stage.label}: unavailable (${stage.reason})`);
  }
  return lines.join('\n');
}

async function getShadowQualityAnalytics({ db = pool, clientId, days = 30 } = {}) {
  const [scores, states, components, reasons, channels, timing, oscillation, blocked, outcomes] = await Promise.all([
    db.query(`SELECT WIDTH_BUCKET(warmth_score,0,100,10) AS bucket, COUNT(*)::int AS count FROM max_decisions WHERE client_id=$1 AND created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY bucket ORDER BY bucket`, [clientId, days]),
    db.query(`SELECT recommended_state, COUNT(*)::int AS count FROM max_decisions WHERE client_id=$1 AND created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY recommended_state ORDER BY count DESC`, [clientId, days]),
    db.query(`SELECT component->>'code' AS code, COUNT(*)::int AS count FROM max_decisions d CROSS JOIN LATERAL jsonb_array_elements(d.score_components) component WHERE d.client_id=$1 AND d.created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY code ORDER BY count DESC LIMIT 20`, [clientId, days]),
    db.query(`SELECT reason AS code, COUNT(*)::int AS count FROM max_decisions d CROSS JOIN LATERAL jsonb_array_elements_text(d.reason_codes) reason WHERE d.client_id=$1 AND d.created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY reason ORDER BY count DESC LIMIT 20`, [clientId, days]),
    db.query(`WITH latest AS (SELECT DISTINCT ON (d.prospect_id) d.prospect_id,d.recommended_state,p.email,p.phone FROM max_decisions d JOIN prospects p ON p.id=d.prospect_id AND p.client_id=d.client_id WHERE d.client_id=$1 ORDER BY d.prospect_id,d.created_at DESC) SELECT COUNT(*) FILTER(WHERE recommended_state='warm' AND email IS NULL AND phone IS NULL)::int AS warm_without_channels FROM latest`, [clientId]),
    db.query(`SELECT metric_name, AVG(metric_value)::numeric(12,2) AS average_ms FROM max_orchestration_metrics WHERE client_id=$1 AND metric_name IN ('signal_to_decision_duration','decision_processing_duration') AND recorded_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY metric_name`, [clientId, days]),
    db.query(`SELECT prospect_id,COUNT(*)::int AS transitions FROM prospect_state_transitions WHERE client_id=$1 AND is_shadow=TRUE AND created_at>=NOW()-($2::int*INTERVAL '1 day') GROUP BY prospect_id HAVING COUNT(*)>=3 ORDER BY transitions DESC LIMIT 50`, [clientId, days]),
    db.query(`WITH latest AS (SELECT DISTINCT ON (prospect_id) prospect_id,recommended_state,next_best_action,created_at FROM max_decisions WHERE client_id=$1 ORDER BY prospect_id,created_at DESC) SELECT COUNT(*)::int AS blocked_warm FROM latest WHERE recommended_state='warm' AND next_best_action IN ('prioritized_enrichment','operator_review')`, [clientId]),
    db.query(`WITH recommendations AS (SELECT DISTINCT ON (prospect_id) prospect_id,recommended_state,created_at FROM max_decisions WHERE client_id=$1 AND created_at>=NOW()-($2::int*INTERVAL '1 day') ORDER BY prospect_id,created_at), outcomes AS (SELECT r.prospect_id,r.recommended_state,BOOL_OR(e.event_type IN ('email_positive_reply','meeting_booked')) AS positive_outcome,BOOL_OR(e.event_type LIKE 'operator_%') AS operator_override FROM recommendations r LEFT JOIN prospect_signal_events e ON e.prospect_id=r.prospect_id AND e.client_id=$1 AND e.event_timestamp>r.created_at GROUP BY r.prospect_id,r.recommended_state) SELECT recommended_state,COUNT(*)::int AS recommendations,COUNT(*) FILTER(WHERE positive_outcome)::int AS later_positive_outcomes,COUNT(*) FILTER(WHERE operator_override)::int AS later_operator_overrides FROM outcomes GROUP BY recommended_state`, [clientId, days]),
  ]);
  return {
    score_distribution: scores.rows,
    recommended_states: states.rows,
    common_components: components.rows,
    common_transition_reasons: reasons.rows,
    warm_without_reachable_channels: number(channels.rows[0]?.warm_without_channels),
    timing: timing.rows,
    oscillating_prospects: oscillation.rows,
    blocked_warm: number(blocked.rows[0]?.blocked_warm),
    outcome_comparison: outcomes.rows,
  };
}

module.exports = { formatShadowDigest, getShadowDigestData, getShadowQualityAnalytics, stageConversion };

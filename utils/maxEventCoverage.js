'use strict';

const pool = require('../db');

const REQUIRED_EVENT_TYPES = Object.freeze([
  'email_human_opened', 'email_proxy_opened', 'email_clicked', 'email_unsubscribed',
  'email_hard_bounced', 'email_soft_bounced', 'email_positive_reply', 'email_out_of_office',
  'icp_score_changed', 'enrichment_succeeded', 'enrichment_failed', 'prospect_discovered',
  'prospect_qualified', 'meeting_booked', 'meeting_cancelled', 'meeting_showed',
  'meeting_no_showed',
]);

const PROVENANCE_SQL = `CASE
  WHEN s.metadata->>'provenance' IN ('live','historical_backfill','manual_recalculation','daily_decay','manual_override','synthetic_smoke')
    THEN s.metadata->>'provenance'
  WHEN s.metadata->>'historical_backfill' = 'true' THEN 'historical_backfill'
  WHEN s.source = 'max_shadow_smoke' OR s.metadata->>'synthetic' = 'true' THEN 'synthetic_smoke'
  ELSE 'live'
END`;

async function buildEventCoverageReport({ clientId = null } = {}, db = pool) {
  const [signals, failures, duplicates] = await Promise.all([
    db.query(`
      SELECT s.client_id,s.event_type,${PROVENANCE_SQL} provenance,
             MIN(s.created_at) first_observed_at,MAX(s.created_at) most_recent_observed_at,
             COUNT(DISTINCT s.id)::int event_count,
             COUNT(DISTINCT s.id)::int successful_ingestion_count,
             COUNT(DISTINCT d.id)::int decision_count,
             (ARRAY_AGG(s.source_record_id ORDER BY s.created_at DESC))[1] last_source_record_id
      FROM prospect_signal_events s
      LEFT JOIN max_decisions d ON d.trigger_event_id=s.id AND d.client_id=s.client_id
      WHERE ($1::int IS NULL OR s.client_id=$1)
      GROUP BY s.client_id,s.event_type,${PROVENANCE_SQL}
    `, [clientId]),
    db.query(`
      SELECT client_id,payload->>'event_type' event_type,
             COALESCE(payload->>'provenance','live') provenance,COUNT(*)::int isolated_failure_count
      FROM agent_log
      WHERE agent_name='max_orchestration' AND action='signal_ingestion_failed'
        AND ($1::int IS NULL OR client_id=$1)
      GROUP BY client_id,payload->>'event_type',COALESCE(payload->>'provenance','live')
    `, [clientId]),
    db.query(`
      SELECT m.client_id,m.dimensions->>'event_type' event_type,
             COALESCE(m.dimensions->>'provenance',
               CASE
                 WHEN s.metadata->>'provenance' IS NOT NULL THEN s.metadata->>'provenance'
                 WHEN s.metadata->>'historical_backfill'='true' THEN 'historical_backfill'
                 WHEN s.source='max_shadow_smoke' OR s.metadata->>'synthetic'='true' THEN 'synthetic_smoke'
                 ELSE 'live'
               END
             ) provenance,
             COALESCE(SUM(m.metric_value),0)::int duplicate_count
      FROM max_orchestration_metrics m
      LEFT JOIN prospect_signal_events s ON s.id=m.signal_event_id
      WHERE m.metric_name='max_duplicate_events_suppressed_total'
        AND ($1::int IS NULL OR m.client_id=$1)
      GROUP BY m.client_id,m.dimensions->>'event_type',COALESCE(m.dimensions->>'provenance',
        CASE
          WHEN s.metadata->>'provenance' IS NOT NULL THEN s.metadata->>'provenance'
          WHEN s.metadata->>'historical_backfill'='true' THEN 'historical_backfill'
          WHEN s.source='max_shadow_smoke' OR s.metadata->>'synthetic'='true' THEN 'synthetic_smoke'
          ELSE 'live'
        END)
    `, [clientId]),
  ]);
  const keyed = new Map();
  const key = row => `${row.client_id ?? 'global'}:${row.event_type}:${row.provenance}`;
  for (const row of signals.rows) keyed.set(key(row), {
    client_id: row.client_id,
    event_type: row.event_type,
    provenance: row.provenance,
    first_observed_timestamp: row.first_observed_at,
    most_recent_observed_timestamp: row.most_recent_observed_at,
    first_observed_live_timestamp: row.provenance === 'live' ? row.first_observed_at : null,
    most_recent_observed_live_timestamp: row.provenance === 'live' ? row.most_recent_observed_at : null,
    event_count: Number(row.event_count || 0),
    live_event_count: row.provenance === 'live' ? Number(row.event_count || 0) : 0,
    successful_ingestion_count: Number(row.successful_ingestion_count || 0),
    isolated_failure_count: 0,
    decision_count: Number(row.decision_count || 0),
    duplicate_count: 0,
    last_source_record_id: row.last_source_record_id || null,
  });
  const ensure = row => {
    const id = key(row);
    if (!keyed.has(id)) keyed.set(id, {
      client_id: row.client_id, event_type: row.event_type, provenance: row.provenance,
      first_observed_timestamp: null, most_recent_observed_timestamp: null,
      first_observed_live_timestamp: null, most_recent_observed_live_timestamp: null,
      event_count: 0, live_event_count: 0, successful_ingestion_count: 0, isolated_failure_count: 0,
      decision_count: 0, duplicate_count: 0, last_source_record_id: null,
    });
    return keyed.get(id);
  };
  for (const row of failures.rows) ensure(row).isolated_failure_count = Number(row.isolated_failure_count || 0);
  for (const row of duplicates.rows) ensure(row).duplicate_count = Number(row.duplicate_count || 0);
  if (clientId != null) {
    for (const eventType of REQUIRED_EVENT_TYPES) {
      for (const provenance of ['live','historical_backfill','synthetic_smoke']) {
        ensure({ client_id: clientId, event_type: eventType, provenance });
      }
    }
  }
  return {
    client_id: clientId,
    generated_at: new Date().toISOString(),
    rows: [...keyed.values()].sort((a, b) =>
      Number(a.client_id || 0) - Number(b.client_id || 0)
      || a.event_type.localeCompare(b.event_type)
      || a.provenance.localeCompare(b.provenance)),
  };
}

module.exports = { PROVENANCE_SQL, REQUIRED_EVENT_TYPES, buildEventCoverageReport };

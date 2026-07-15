'use strict';

require('dotenv').config();
const pool = require('../db');
const { persistNormalizedSignal, ingestNormalizedSignal } = require('../utils/maxSignalIngestion');
const { assertAllowed, boundedInteger, optionalPositiveInteger, optionalTimestamp, tokenizeArgs } = require('../utils/maxCli');

const DURABLE_TYPES = new Set([
  'email_positive_reply','email_meaningful_reply','email_negative_reply','email_unsubscribed',
  'email_hard_bounced_confirmed_invalid','meeting_booked','meeting_showed',
]);

function parseArgs(argv = process.argv.slice(2), now = new Date()) {
  const parsed = tokenizeArgs(argv);
  assertAllowed(parsed, {
    values: ['--client-id','--from','--to','--limit','--cursor'],
    flags: ['--apply'],
  });
  const defaultFrom = new Date(now.getTime() - 14 * 86400000).toISOString();
  const from = optionalTimestamp(parsed.values.get('--from'), '--from') || defaultFrom;
  const to = optionalTimestamp(parsed.values.get('--to'), '--to') || now.toISOString();
  if (new Date(from) >= new Date(to)) throw new Error('--from must be earlier than --to');
  return {
    apply: parsed.flags.has('--apply'),
    clientId: optionalPositiveInteger(parsed.values.get('--client-id'), '--client-id'),
    from, to,
    limit: boundedInteger(parsed.values.get('--limit'), '--limit', { defaultValue: 500, max: 5000 }),
    cursor: parsed.values.get('--cursor') || null,
  };
}

function cursorFor(row) {
  return `${new Date(row.event_timestamp).toISOString()}|${row.source}|${row.source_record_id}|${row.event_type}`;
}

function normalizeEmailRow(row) {
  const raw = String(row.raw_type || '').toLowerCase();
  let eventType = ({ clicked: 'email_clicked', click: 'email_clicked', unsubscribed: 'email_unsubscribed', invalid: 'email_hard_bounced_confirmed_invalid' })[raw];
  if (['opened','open','opened_proxy'].includes(raw)) {
    eventType = row.open_source === 'human' ? 'email_human_opened'
      : row.open_source === 'proxy' ? 'email_proxy_opened' : 'email_unknown_opened';
  }
  if (!eventType) return null;
  return {
    client_id: row.client_id, prospect_id: row.prospect_id, event_type: eventType,
    event_timestamp: row.event_timestamp, source: 'brevo', source_record_id: String(row.source_record_id),
    metadata: { open_source: row.open_source || null, open_source_reason: row.open_source_reason || null, historical_backfill: true },
  };
}

function parseOutcome(value) {
  if (value && typeof value === 'object') return value;
  try { return JSON.parse(value || '{}'); } catch (_) { return {}; }
}

function normalizeTouchpointRow(row) {
  const action = String(row.raw_type || '').toLowerCase();
  const outcome = parseOutcome(row.payload);
  const classification = String(outcome.classification || '').toLowerCase();
  let eventType = null;
  if (action === 'out_of_office' || ['out_of_office','automated'].includes(classification)) eventType = 'email_out_of_office';
  else if (action === 'unsubscribed' || classification === 'unsubscribe') eventType = 'email_unsubscribed';
  else if (['inbound_reply','inbound','reply','email_reply'].includes(action)) {
    eventType = classification === 'interested' ? 'email_positive_reply'
      : classification === 'negative' ? 'email_negative_reply'
        : 'email_meaningful_reply';
  }
  if (!eventType) return null;
  return {
    client_id: row.client_id, prospect_id: row.prospect_id, event_type: eventType,
    event_timestamp: row.event_timestamp, source: 'touchpoints', source_record_id: String(row.source_record_id),
    metadata: { classification: classification || null, historical_backfill: true },
  };
}

async function sourceRows(db, options) {
  const params = [options.clientId, options.from, options.to, options.limit];
  const sources = [];
  const query = async (source, sql, normalize) => {
    try {
      const result = await db.query(sql, params);
      for (const row of result.rows) {
        const signal = normalize(row);
        if (signal) sources.push(signal);
      }
    } catch (error) {
      if (!['42P01','42703'].includes(error.code)) throw error;
      sources.push({ unavailable_source: source, error: error.message });
    }
  };
  await query('email_events', `
    SELECT event_id AS source_record_id, prospect_id, client_id, event_type AS raw_type,
           event_at AS event_timestamp, open_source::text, open_source_reason
    FROM email_events
    WHERE prospect_id IS NOT NULL AND ($1::int IS NULL OR client_id=$1)
      AND event_at >= $2::timestamptz AND event_at < $3::timestamptz
      AND event_type IN ('opened','open','opened_proxy','clicked','click','unsubscribed','invalid')
    ORDER BY event_at, event_id LIMIT $4
  `, normalizeEmailRow);
  await query('touchpoints', `
    SELECT id::text AS source_record_id, prospect_id, client_id, action_type AS raw_type,
           created_at AS event_timestamp, outcome AS payload
    FROM touchpoints
    WHERE prospect_id IS NOT NULL AND ($1::int IS NULL OR client_id=$1)
      AND created_at >= $2::timestamptz AND created_at < $3::timestamptz
      AND action_type IN ('inbound_reply','inbound','reply','email_reply','out_of_office','unsubscribed')
    ORDER BY created_at, id LIMIT $4
  `, normalizeTouchpointRow);
  await query('icp_score_history', `
    SELECT h.id::text AS source_record_id, h.prospect_id, p.client_id, 'icp_score_changed' AS event_type,
           h.created_at AS event_timestamp, h.old_score, h.new_score, h.reason
    FROM icp_score_history h JOIN prospects p ON p.id=h.prospect_id
    WHERE ($1::int IS NULL OR p.client_id=$1)
      AND h.created_at >= $2::timestamptz AND h.created_at < $3::timestamptz
    ORDER BY h.created_at, h.id LIMIT $4
  `, row => ({
    client_id: row.client_id, prospect_id: row.prospect_id, event_type: row.event_type,
    event_timestamp: row.event_timestamp, source: 'icp_score_history', source_record_id: row.source_record_id,
    metadata: { old_score: row.old_score, new_score: row.new_score, delta: Number(row.new_score || 0)-Number(row.old_score || 0), reason: row.reason, historical_backfill: true },
  }));
  await query('enrichment_agent_log', `
    SELECT id::text AS source_record_id, prospect_id, client_id, action AS raw_type,
           ran_at AS event_timestamp, status, payload
    FROM agent_log
    WHERE prospect_id IS NOT NULL AND ($1::int IS NULL OR client_id=$1)
      AND ran_at >= $2::timestamptz AND ran_at < $3::timestamptz
      AND action='enrichment_attempt'
    ORDER BY ran_at, id LIMIT $4
  `, row => {
    const payload = parseOutcome(row.payload);
    const successful = row.status === 'success' && Boolean(payload.updated?.email || payload.updated?.phone || payload.resolved || payload.verified_email);
    return {
      client_id: row.client_id, prospect_id: row.prospect_id,
      event_type: successful ? 'enrichment_succeeded' : 'enrichment_failed',
      event_timestamp: row.event_timestamp, source: 'enrichment', source_record_id: row.source_record_id,
      metadata: { provider: payload.provider || 'historical_agent_log', status: row.status, historical_backfill: true },
    };
  });
  await query('prospects.booked_at', `
    SELECT ('prospect:'||id::text||':booked') AS source_record_id, id AS prospect_id, client_id,
           booked_at AS event_timestamp
    FROM prospects
    WHERE booked_at IS NOT NULL AND ($1::int IS NULL OR client_id=$1)
      AND booked_at >= $2::timestamptz AND booked_at < $3::timestamptz
    ORDER BY booked_at, id LIMIT $4
  `, row => ({
    client_id: row.client_id, prospect_id: row.prospect_id, event_type: 'meeting_booked',
    event_timestamp: row.event_timestamp, source: 'prospects_booked_at', source_record_id: row.source_record_id,
    metadata: { historical_backfill: true, canonical_field: 'prospects.booked_at' },
  }));
  return sources;
}

async function run(options = parseArgs(), db = pool) {
  const rows = await sourceRows(db, options);
  const unavailable = rows.filter(row => row.unavailable_source);
  const signals = rows.filter(row => !row.unavailable_source)
    .sort((a,b) => cursorFor(a).localeCompare(cursorFor(b)))
    .filter(row => !options.cursor || cursorFor(row) > options.cursor)
    .slice(0, options.limit);
  const report = {
    mode: options.apply ? 'shadow-write' : 'dry-run', from: options.from, to: options.to,
    client_id: options.clientId, scanned: signals.length, inserted: 0, duplicates: 0, decisions_generated: 0,
    temporary_scoring_signals: 0, durable_signals: 0, by_source: {}, by_event_type: {},
    unavailable_sources: unavailable.map(row => ({ source: row.unavailable_source, error: row.error })),
    errors: [], next_cursor: signals.length ? cursorFor(signals[signals.length-1]) : options.cursor,
    side_effects: { status_updates: 0, sends: 0, sequence_changes: 0, tasks: 0, enrichment_retries: 0 },
  };
  for (const signal of signals) {
    report.by_source[signal.source] = (report.by_source[signal.source] || 0) + 1;
    report.by_event_type[signal.event_type] = (report.by_event_type[signal.event_type] || 0) + 1;
    if (DURABLE_TYPES.has(signal.event_type)) report.durable_signals++; else report.temporary_scoring_signals++;
    if (!options.apply) continue;
    try {
      const result = await ingestNormalizedSignal(signal, { db, evaluate: true });
      if (result.inserted) report.inserted++; else report.duplicates++;
      if (result.evaluated && !result.duplicate) report.decisions_generated++;
    } catch (error) {
      report.errors.push({ source: signal.source, source_record_id: signal.source_record_id, error: error.message });
    }
  }
  return report;
}

module.exports = { DURABLE_TYPES, cursorFor, normalizeEmailRow, normalizeTouchpointRow, parseArgs, run, sourceRows };

if (require.main === module) {
  run().then(report => {
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.errors.length ? 1 : 0;
  }).catch(error => { console.error(error.stack || error.message); process.exitCode=1; })
    .finally(() => pool.end());
}

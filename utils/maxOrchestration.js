const crypto = require('crypto');
const pool = require('../db');
const { loadMaxOrchestrationConfig, withProspectTier } = require('../config/maxOrchestration');
const { calculateWarmthScore } = require('./maxWarmthScoring');
const { determineStateDecision } = require('./maxStateDecision');

const EMAIL_EVENT_MAP = Object.freeze({
  opened: 'email_human_opened',
  open: 'email_human_opened',
  opened_proxy: 'email_proxy_opened',
  clicked: 'email_clicked',
  click: 'email_clicked',
  replied: 'email_replied',
  soft_bounce: 'email_soft_bounced',
  hard_bounce: 'email_hard_bounced',
  unsubscribed: 'email_unsubscribed',
  invalid: 'contact_invalid',
});

function mapLegacyStatus(status, doNotContact = false) {
  const legacy = String(status || '').trim().toLowerCase();
  if (doNotContact && !['closed'].includes(legacy)) return 'disqualified';
  return ({
    cold: 'cold', contacted: 'heating', warm: 'warm', hot: 'hot', closed: 'engaged',
    auto_responder: 'nurture', bounced: 'null', do_not_email: 'disqualified',
    disqualified: 'disqualified', dead: 'recycle',
  })[legacy] || 'cold';
}

function normalizeStoredEmailEvent(row) {
  let type = EMAIL_EVENT_MAP[String(row.event_type || '').toLowerCase()] || null;
  const source = String(row.open_source || '').toLowerCase();
  if (['opened', 'open'].includes(String(row.event_type || '').toLowerCase())) {
    type = source === 'human' ? 'email_human_opened' : source === 'proxy' ? 'email_proxy_opened' : null;
  }
  if (!type) return null;
  return {
    id: `email_events:${row.id}`,
    event_type: type,
    event_timestamp: row.event_at,
    source: 'email_events',
    source_record_id: String(row.id),
    metadata: { open_source: row.open_source || null, verified: type === 'email_clicked' },
  };
}

function normalizeTouchpoint(row) {
  const action = String(row.action_type || '').toLowerCase();
  let outcome = typeof row.outcome === 'object' && row.outcome !== null ? row.outcome : {};
  if (typeof row.outcome === 'string') {
    try { outcome = JSON.parse(row.outcome); } catch (_) { outcome = {}; }
  }
  let type = null;
  if (action === 'out_of_office') type = 'email_out_of_office';
  else if (action === 'unsubscribed') type = 'email_unsubscribed';
  else if (['inbound_reply', 'inbound', 'reply', 'email_reply'].includes(action)) {
    const classification = String(outcome?.classification || '').toLowerCase();
    if (classification === 'interested') type = 'email_positive_reply';
    else if (['negative'].includes(classification)) type = 'email_negative_reply';
    else if (['out_of_office', 'automated'].includes(classification)) type = 'email_out_of_office';
    else if (classification === 'unsubscribe') type = 'email_unsubscribed';
    else type = 'email_meaningful_reply';
  }
  if (!type) return null;
  return {
    id: `touchpoints:${row.id}`,
    event_type: type,
    event_timestamp: row.created_at,
    source: 'touchpoints',
    source_record_id: String(row.id),
    metadata: { classification: outcome?.classification || null },
  };
}

async function loadProspectContext(db, prospectId, clientId, { includeLegacySignals = false } = {}) {
  const prospectResult = await db.query(`
    SELECT p.*, c.name AS company_name, c.industry AS company_industry
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1 AND p.client_id = $2
  `, [prospectId, clientId]);
  const prospect = prospectResult.rows[0];
  if (!prospect) throw new Error(`Prospect ${prospectId} not found for client ${clientId}`);
  if (prospect.is_synthetic) {
    const error = new Error('Synthetic prospects are excluded from Max orchestration');
    error.code = 'SYNTHETIC_PROSPECT_EXCLUDED';
    throw error;
  }

  const normalized = await db.query(`
    SELECT signal.id, signal.event_type, signal.event_timestamp, signal.source, signal.source_record_id, signal.metadata
    FROM prospect_signal_events signal
    LEFT JOIN email_events raw_email
      ON signal.source = 'brevo'
     AND raw_email.event_id = signal.source_record_id
    WHERE signal.prospect_id = $1 AND signal.client_id = $2
      AND signal.event_timestamp >= NOW() - INTERVAL '120 days'
      AND NOT (
        signal.event_type = 'email_human_opened'
        AND signal.source = 'brevo'
        AND COALESCE(raw_email.open_source::text, 'unknown') <> 'human'
      )
    ORDER BY signal.event_timestamp ASC, signal.id ASC
  `, [prospectId, clientId]).catch(() => ({ rows: [] }));

  if (!includeLegacySignals) {
    return {
      prospect,
      signals: normalized.rows.map(row => ({
        id: row.id,
        event_type: row.event_type,
        event_timestamp: row.event_timestamp,
        source: row.source,
        source_record_id: row.source_record_id,
        metadata: row.metadata || {},
      })),
    };
  }

  const [email, touches, icp] = await Promise.all([
    db.query(`
      SELECT id, event_type, event_at, open_source
      FROM email_events
      WHERE prospect_id = $1 AND client_id = $2 AND event_at >= NOW() - INTERVAL '120 days'
      ORDER BY event_at ASC, id ASC
    `, [prospectId, clientId]).catch(() => ({ rows: [] })),
    db.query(`
      SELECT id, action_type, outcome, created_at
      FROM touchpoints
      WHERE prospect_id = $1 AND client_id = $2 AND created_at >= NOW() - INTERVAL '120 days'
      ORDER BY created_at ASC, id ASC
    `, [prospectId, clientId]),
    db.query(`
      SELECT id, old_score, new_score, created_at
      FROM icp_score_history
      WHERE prospect_id = $1 AND created_at >= NOW() - INTERVAL '120 days'
      ORDER BY created_at ASC, id ASC
    `, [prospectId]).catch(() => ({ rows: [] })),
  ]);

  const signalsByIdentity = new Map();
  const legacySignals = [
    ...email.rows.map(normalizeStoredEmailEvent).filter(Boolean),
    ...touches.rows.map(normalizeTouchpoint).filter(Boolean),
    ...icp.rows.map(row => ({
      id: `icp_score_history:${row.id}`,
      event_type: 'icp_score_changed',
      event_timestamp: row.created_at,
      source: 'icp_score_history',
      source_record_id: String(row.id),
      metadata: { old_score: row.old_score, new_score: row.new_score, delta: Number(row.new_score || 0) - Number(row.old_score || 0) },
    })),
  ];
  for (const signal of [...legacySignals, ...normalized.rows.map(row => ({
    id: row.id,
    event_type: row.event_type,
    event_timestamp: row.event_timestamp,
    source: row.source,
    source_record_id: row.source_record_id,
    metadata: row.metadata || {},
  }))]) {
    const identity = `${signal.source}:${signal.source_record_id}:${signal.event_type}`;
    signalsByIdentity.set(identity, signal);
  }
  return { prospect, signals: [...signalsByIdentity.values()] };
}

function idempotencyKey(prospectId, triggerEventId, decisionVersion, now) {
  const trigger = triggerEventId || `daily:${now.toISOString().slice(0, 10)}`;
  return crypto.createHash('sha256').update(`${prospectId}:${trigger}:${decisionVersion}`).digest('hex');
}

function autonomyLevel(action) {
  if (action.includes('operator') || action.includes('reply_required') || action.includes('hot_prospect')) return 'operator_required';
  if (['pause_cold_sequence', 'start_warm_sequence', 'retry_enrichment', 'schedule_recycle'].includes(action)) return 'guardrailed_autonomous';
  if (action === 'pause_automated_outreach' || action === 'stop_automated_sequences') return 'guardrailed_autonomous';
  return 'autonomous';
}

function callerTransactionClient(db, transactionContext) {
  if (!transactionContext) return null;
  if (transactionContext.transactionManagedByCaller !== true) {
    throw new Error('Caller transaction context requires transactionManagedByCaller=true');
  }
  if (!transactionContext.client || typeof transactionContext.client.query !== 'function') {
    throw new Error('Caller transaction context requires a query-capable client');
  }
  if (db !== transactionContext.client) {
    throw new Error('Caller transaction context client must be the same client used for orchestration reads');
  }
  return transactionContext.client;
}

async function recordShadowDecision(
  { db = pool, prospect, scoreResult, decision, config, triggerEvent, key, startedAt, now },
  transactionContext = null
) {
  const suppliedClient = callerTransactionClient(db, transactionContext);
  if (!suppliedClient && (!db || typeof db.connect !== 'function')) {
    throw new Error('Internally managed shadow decisions require a database pool with connect()');
  }
  const client = suppliedClient || await db.connect();
  const ownsClient = !suppliedClient;
  const decisionId = crypto.randomUUID();
  try {
    if (ownsClient) await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [String(prospect.id)]);
    const existing = await client.query(
      'SELECT * FROM max_decisions WHERE client_id = $1 AND idempotency_key = $2',
      [prospect.client_id, key]
    );
    if (existing.rows[0]) {
      if (ownsClient) await client.query('COMMIT');
      return { duplicate: true, decision: existing.rows[0], score: scoreResult };
    }

    if (triggerEvent?.id) {
      await client.query(`
        INSERT INTO prospect_signal_events
          (id, client_id, prospect_id, company_id, event_type, event_timestamp, source, source_record_id, metadata)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
        ON CONFLICT DO NOTHING
      `, [
        String(triggerEvent.id), prospect.client_id, prospect.id, prospect.company_id || null,
        triggerEvent.event_type, triggerEvent.event_timestamp, triggerEvent.source || 'max_orchestration',
        triggerEvent.source_record_id || String(triggerEvent.id), JSON.stringify(triggerEvent.metadata || {}),
      ]);
    }

    const duration = Date.now() - startedAt;
    await client.query(`
      INSERT INTO max_decisions (
        id, client_id, prospect_id, company_id, trigger_event_type, trigger_event_id,
        idempotency_key, decision_version, score_version, current_state, recommended_state,
        warmth_score, score_components, reason_codes, reason_summary, next_best_action,
        actions, operator_required, operator_priority, is_shadow, config_snapshot, processing_duration_ms, created_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14::jsonb,$15,$16,$17::jsonb,$18,$19,TRUE,$20::jsonb,$21,$22)
    `, [
      decisionId, prospect.client_id, prospect.id, prospect.company_id || null,
      triggerEvent?.event_type || 'scheduled_recalculation', triggerEvent?.id || null, key,
      decision.decision_version, scoreResult.score_version, decision.current_state, decision.recommended_state,
      scoreResult.score, JSON.stringify(scoreResult.components), JSON.stringify(decision.reason_codes),
      decision.reason_summary, decision.next_best_action, JSON.stringify(decision.actions),
      decision.operator_required, decision.operator_priority, JSON.stringify(config), duration, now,
    ]);

    if (decision.transition_recommended) {
      await client.query(`
        INSERT INTO prospect_state_transitions (
          client_id, prospect_id, decision_id, from_state, to_state, warmth_score, reason_codes,
          reason_summary, trigger_event_type, trigger_event_id, decision_source, action_selected,
          operator_required, is_shadow, applied, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,'max_autonomous',$11,$12,TRUE,FALSE,$13)
      `, [
        prospect.client_id, prospect.id, decisionId, decision.current_state, decision.recommended_state,
        scoreResult.score, JSON.stringify(decision.reason_codes), decision.reason_summary,
        triggerEvent?.event_type || 'scheduled_recalculation', triggerEvent?.id || null,
        decision.next_best_action, decision.operator_required, now,
      ]);
    }

    for (const action of decision.actions) {
      const actionKey = `${decisionId}:${action}:${prospect.id}`;
      await client.query(`
        INSERT INTO max_actions (
          id, client_id, prospect_id, decision_id, action_type, action_status, autonomy_level,
          idempotency_key, input_payload, output_payload, error_code, error_message, completed_at, created_at
        ) VALUES ($1,$2,$3,$4,$5,'skipped',$6,$7,$8::jsonb,$9::jsonb,'SHADOW_MODE',$10,$11,$11)
      `, [
        crypto.randomUUID(), prospect.client_id, prospect.id, decisionId, action, autonomyLevel(action), actionKey,
        JSON.stringify({ recommended_state: decision.recommended_state }),
        JSON.stringify({ executed: false }), 'Phase 2 records recommendations only; no side effect was executed.', now,
      ]);
    }
    await client.query(`
      UPDATE prospects
      SET warmth_score = $1,
          warmth_score_updated_at = $2,
          warmth_score_version = $3,
          last_meaningful_signal_at = COALESCE($4::timestamptz, last_meaningful_signal_at),
          last_human_open_at = COALESCE($5::timestamptz, last_human_open_at),
          last_reply_at = COALESCE($6::timestamptz, last_reply_at),
          last_positive_reply_at = COALESCE($7::timestamptz, last_positive_reply_at),
          downgrade_candidate_since = $8::timestamptz,
          updated_at = updated_at
      WHERE id = $9 AND client_id = $10
    `, [
      scoreResult.score, now, scoreResult.score_version, scoreResult.last_meaningful_signal_at,
      scoreResult.last_human_open_at, scoreResult.last_reply_at, scoreResult.last_positive_reply_at,
      decision.downgrade_candidate_since, prospect.id, prospect.client_id,
    ]);
    const metricRows = [
      ['max_decisions_total', 1],
      ['decision_processing_duration', duration],
      ...(decision.transition_recommended ? [['max_state_transition_recommendations_total', 1]] : []),
      ...(decision.actions.length ? [['max_action_recommendations_total', decision.actions.length]] : []),
    ];
    for (const [metricName, metricValue] of metricRows) {
      await client.query(`
        INSERT INTO max_orchestration_metrics
          (client_id, metric_name, metric_value, prospect_id, signal_event_id, decision_id, dimensions)
        VALUES ($1,$2,$3,$4,$5,$6,'{}'::jsonb)
      `, [prospect.client_id, metricName, metricValue, prospect.id, triggerEvent?.id || null, decisionId]);
    }
    if (ownsClient) await client.query('COMMIT');
    return { duplicate: false, decision: { id: decisionId, ...decision, is_shadow: true, created_at: now }, score: scoreResult };
  } catch (error) {
    if (ownsClient) await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    if (ownsClient) client.release();
  }
}

async function calculateProspectShadow({ db = pool, prospectId, clientId, triggerEvent = null, clientConfig = {}, env = process.env, now = new Date(), includeLegacySignals = false, ignoreFeatureFlags = false }) {
  const config = loadMaxOrchestrationConfig({ env, clientOverrides: clientConfig.max_orchestration_config });
  if (!ignoreFeatureFlags && (!config.enabled || !config.flags.max_scoring_enabled)) {
    return { skipped: true, reason: !config.enabled ? 'max_orchestration_disabled' : 'max_scoring_disabled' };
  }
  if (!config.flags.max_shadow_mode) throw new Error('Phase 2 only supports shadow mode');
  const context = await loadProspectContext(db, prospectId, clientId, { includeLegacySignals });
  const prospect = withProspectTier({
    ...context.prospect,
    lifecycle_state: context.prospect.lifecycle_state || mapLegacyStatus(context.prospect.status, context.prospect.do_not_contact),
  }, clientConfig);
  const triggerAlreadyLoaded = triggerEvent?.id && context.signals.some(signal => String(signal.id) === String(triggerEvent.id));
  const signals = triggerEvent && !triggerAlreadyLoaded ? [...context.signals, triggerEvent] : context.signals;
  const scoreResult = calculateWarmthScore({ prospect, signals, config, now });
  const decision = determineStateDecision({ prospect, scoreResult, signals, config, now });
  return { config, prospect, signals, scoreResult, decision };
}

async function evaluateProspectShadow(args) {
  const startedAt = Date.now();
  const calculated = await calculateProspectShadow(args);
  if (calculated.skipped) return calculated;
  const { db = pool, triggerEvent = null, now = new Date() } = args;
  const { config, prospect, scoreResult, decision } = calculated;
  const key = idempotencyKey(prospect.id, triggerEvent?.id, decision.decision_version, now);
  return recordShadowDecision(
    { db, prospect, scoreResult, decision, config, triggerEvent, key, startedAt, now },
    args.transactionContext || null
  );
}

module.exports = {
  EMAIL_EVENT_MAP,
  autonomyLevel,
  callerTransactionClient,
  calculateProspectShadow,
  evaluateProspectShadow,
  idempotencyKey,
  loadProspectContext,
  mapLegacyStatus,
  normalizeStoredEmailEvent,
  normalizeTouchpoint,
  recordShadowDecision,
};

const crypto = require('crypto');
const pool = require('../db');
const { TERMINAL_STATES, normalizeLifecycleState } = require('./maxStateDecision');
const { mapLegacyStatus } = require('./maxOrchestration');
const { recordMaxMetric } = require('./maxOrchestrationObservability');

const OVERRIDE_SIGNAL_TYPES = Object.freeze({
  warm: 'operator_marked_warm',
  hot: 'operator_marked_hot',
  engaged: 'operator_marked_engaged',
  recycle: 'operator_recycled',
  disqualified: 'operator_disqualified',
  null: 'operator_nulled',
  cold: 'operator_restored_cold',
  heating: 'operator_restored_heating',
  nurture: 'operator_moved_nurture',
});

function overrideError(message, statusCode = 400) {
  const error = new Error(message);
  error.statusCode = statusCode;
  return error;
}

async function applyManualLifecycleOverride({
  db = pool,
  prospectId,
  clientId,
  requestedState,
  reason,
  source = 'dashboard',
  operator = {},
  confirmTerminalRestore = false,
  now = new Date(),
}) {
  const target = normalizeLifecycleState(requestedState);
  const cleanReason = String(reason || '').trim();
  if (cleanReason.length < 5) throw overrideError('Override reason must be at least 5 characters');
  const client = typeof db.connect === 'function' ? await db.connect() : db;
  const ownsClient = client !== db;
  const overrideId = crypto.randomUUID();
  const signalId = `manual_override:${overrideId}`;
  try {
    await client.query('BEGIN');
    await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [String(prospectId)]);
    const currentResult = await client.query(`
      SELECT id, client_id, company_id, lifecycle_state, status, do_not_contact
      FROM prospects WHERE id = $1 AND client_id = $2 FOR UPDATE
    `, [prospectId, clientId]);
    const prospect = currentResult.rows[0];
    if (!prospect) throw overrideError('Prospect not found', 404);
    const fromState = prospect.lifecycle_state || mapLegacyStatus(prospect.status, prospect.do_not_contact);
    if (fromState === target) throw overrideError(`Prospect is already ${target}`);
    const terminalRestore = TERMINAL_STATES.has(fromState) && !TERMINAL_STATES.has(target);
    if (terminalRestore && confirmTerminalRestore !== true) {
      throw overrideError('Restoring a terminal lifecycle state requires confirm_terminal_restore=true');
    }
    const eventType = OVERRIDE_SIGNAL_TYPES[target];
    await client.query(`
      INSERT INTO prospect_signal_events
        (id, client_id, prospect_id, company_id, event_type, event_timestamp, source, source_record_id, metadata)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)
    `, [
      signalId, clientId, prospectId, prospect.company_id || null, eventType, now,
      'operator_manual', overrideId,
      JSON.stringify({ provenance: 'manual_override', requested_state: target, from_state: fromState, reason: cleanReason, operator_user_id: operator.id || null }),
    ]);
    await client.query(`
      INSERT INTO manual_lifecycle_overrides (
        id, client_id, prospect_id, signal_event_id, operator_user_id, operator_identity,
        from_state, requested_state, reason, source, terminal_restore, created_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
    `, [
      overrideId, clientId, prospectId, signalId, operator.id || null,
      operator.email || operator.name || null, fromState, target, cleanReason,
      String(source || 'dashboard').slice(0, 100), terminalRestore, now,
    ]);
    await client.query(`
      INSERT INTO prospect_state_transitions (
        client_id, prospect_id, from_state, to_state, warmth_score, reason_codes,
        reason_summary, trigger_event_type, trigger_event_id, decision_source,
        action_selected, operator_required, is_shadow, applied, created_at
      ) VALUES ($1,$2,$3,$4,NULL,$5::jsonb,$6,$7,$8,'operator_manual','manual_lifecycle_override',TRUE,FALSE,TRUE,$9)
    `, [
      clientId, prospectId, fromState, target,
      JSON.stringify(['OPERATOR_MANUAL_OVERRIDE']), cleanReason, eventType, signalId, now,
    ]);
    await client.query(`
      UPDATE prospects
      SET previous_lifecycle_state = $1,
          lifecycle_state = $2,
          state_changed_at = $3,
          state_reason_codes = '["OPERATOR_MANUAL_OVERRIDE"]'::jsonb,
          state_reason_summary = $4,
          downgrade_candidate_since = NULL,
          updated_at = NOW()
      WHERE id = $5 AND client_id = $6
    `, [fromState, target, now, cleanReason, prospectId, clientId]);
    await client.query('COMMIT');
    await recordMaxMetric('max_manual_overrides_total', {
      db, clientId, prospectId, signalEventId: signalId,
      dimensions: { from_state: fromState, requested_state: target, terminal_restore: terminalRestore },
    }).catch(() => {});
    return {
      id: overrideId,
      prospect_id: prospectId,
      previous_state: fromState,
      lifecycle_state: target,
      reason: cleanReason,
      terminal_restore: terminalRestore,
      signal_event_id: signalId,
      created_at: now.toISOString(),
    };
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    if (ownsClient) client.release();
  }
}

module.exports = { OVERRIDE_SIGNAL_TYPES, applyManualLifecycleOverride, overrideError };

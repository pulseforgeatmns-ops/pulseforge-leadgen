'use strict';

const pool = require('../db');

function sequenceType(name, explicit) {
  if (['cold','warm','nurture'].includes(String(explicit || '').toLowerCase())) return String(explicit).toLowerCase();
  const text = String(name || '').toLowerCase();
  if (text.includes('warm')) return 'warm';
  if (text.includes('nurture')) return 'nurture';
  return 'cold';
}

function resolveSequenceState(row) {
  const activeSequence = row.active_sequence_id || row.latest_sequence || row.client_sequence || null;
  const type = sequenceType(activeSequence, row.active_sequence_type);
  const hasReply = Boolean(row.has_reply);
  const terminal = Boolean(row.do_not_contact) || ['dead','disqualified','closed'].includes(String(row.status || '').toLowerCase());
  if (terminal || hasReply || row.has_terminal_email_event) {
    return { prospect_id: row.id, active_sequence: null, active_sequence_type: type, enrollment_status: 'suppressed', next_scheduled_send_at: null, source: 'emmett_operational_fields', confidence: 'high' };
  }
  if (row.email_sequence_completed_at) {
    return { prospect_id: row.id, active_sequence: activeSequence, active_sequence_type: type, enrollment_status: 'completed', next_scheduled_send_at: null, source: 'prospects.email_sequence_completed_at', confidence: 'high' };
  }
  if (row.has_pending_send) {
    return { prospect_id: row.id, active_sequence: activeSequence, active_sequence_type: type, enrollment_status: 'active', next_scheduled_send_at: row.pending_send_at || row.next_touch_at || null, source: 'agent_log.email_pending', confidence: 'high' };
  }
  if (row.last_contacted_at || row.next_touch_at) {
    return { prospect_id: row.id, active_sequence: activeSequence, active_sequence_type: type, enrollment_status: 'active', next_scheduled_send_at: row.next_touch_at || null, source: 'prospects.next_touch_at', confidence: 'medium' };
  }
  return { prospect_id: row.id, active_sequence: activeSequence, active_sequence_type: type, enrollment_status: 'not_started', next_scheduled_send_at: null, source: 'emmett_eligibility_inference', confidence: 'medium' };
}

async function getSequenceState(prospectId, clientId, db = pool) {
  const result = await db.query(`
    SELECT p.id, p.status, p.do_not_contact, p.last_contacted_at, p.next_touch_at,
           p.email_sequence_completed_at, p.active_sequence_id, p.active_sequence_type,
           c.email_sequence AS client_sequence,
           latest.payload->>'sequence' AS latest_sequence,
           EXISTS (
             SELECT 1 FROM agent_log pending
             WHERE pending.client_id=p.client_id AND pending.prospect_id=p.id
               AND pending.agent_name='emmett' AND pending.action='email_pending' AND pending.status='pending'
           ) AS has_pending_send,
           (SELECT MIN(COALESCE(NULLIF(pending.payload->>'scheduled_for','')::timestamptz, p.next_touch_at))
            FROM agent_log pending WHERE pending.client_id=p.client_id AND pending.prospect_id=p.id
              AND pending.agent_name='emmett' AND pending.action='email_pending' AND pending.status='pending') AS pending_send_at,
           EXISTS (SELECT 1 FROM touchpoints t WHERE t.client_id=p.client_id AND t.prospect_id=p.id
             AND t.action_type IN ('inbound_reply','inbound','reply','email_reply')) AS has_reply,
           EXISTS (SELECT 1 FROM touchpoints t WHERE t.client_id=p.client_id AND t.prospect_id=p.id
             AND t.action_type IN ('email_hard_bounce','email_unsubscribed','email_spam')) AS has_terminal_email_event
    FROM prospects p
    JOIN clients c ON c.id=p.client_id
    LEFT JOIN LATERAL (
      SELECT payload FROM agent_log sent
      WHERE sent.client_id=p.client_id AND sent.prospect_id=p.id
        AND sent.agent_name='emmett' AND sent.action='email_sent'
      ORDER BY sent.ran_at DESC LIMIT 1
    ) latest ON TRUE
    WHERE p.id=$1::uuid AND p.client_id=$2::int
  `, [prospectId, clientId]);
  if (!result.rows[0]) throw new Error(`Prospect ${prospectId} not found for client ${clientId}`);
  return resolveSequenceState(result.rows[0]);
}

module.exports = { getSequenceState, resolveSequenceState, sequenceType };

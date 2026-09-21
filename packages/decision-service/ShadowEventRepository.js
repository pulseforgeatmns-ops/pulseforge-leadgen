'use strict';

// The stored projection preserves every SPEC-JEV-001 field, including the two
// optional privacy-controlled fields. No prompt or production state is added.
const FIELDS = Object.freeze([
  'event', 'spec', 'schema_version', 'decision_id', 'mode', 'source',
  'session_id', 'tenant_id', 'message_index', 'message_chars', 'message_truncated',
  'provider', 'requested_provider', 'requested_model', 'model', 'mission_id',
  'current_route', 'routing_latency_ms', 'status', 'intent', 'confidence',
  'mission_bound_probability', 'approval_probability', 'inspection_probability',
  'requires_human_clarification', 'risk_if_misrouted', 'recommended_route',
  'route_matches', 'comparison', 'latency_ms', 'fallback_provider',
  'fallback_reason', 'errors', 'timestamp', 'raw_redacted_response',
]);
const JSON_FIELDS = new Set(['current_route', 'errors', 'raw_redacted_response']);
const INSERT = `INSERT INTO decision_shadow_events (${FIELDS.join(', ')})
  VALUES (${FIELDS.map((_, i) => `$${i + 1}`).join(', ')})
  ON CONFLICT (decision_id) DO NOTHING`;

function insertShadowEvent(db, row) {
  const values = FIELDS.map(field => row[field] == null ? null
    : JSON_FIELDS.has(field) ? JSON.stringify(row[field]) : row[field]);
  return db.query(INSERT, values);
}

function normalizeShadowEvent(row) {
  return {
    ...row,
    timestamp: row.timestamp instanceof Date ? row.timestamp.toISOString() : row.timestamp,
  };
}

function reviewOptions({ limit = 50, tenantId = null, filter = 'all' } = {}) {
  if (!Number.isInteger(limit) || limit < 1 || limit > 500) throw new Error('limit must be an integer from 1 to 500');
  if (!['all', 'mismatches', 'errors', 'warnings'].includes(filter)) {
    throw new Error('filter must be all, mismatches, errors, or warnings');
  }
  if (tenantId !== null && (typeof tenantId !== 'string' || !tenantId.trim() || tenantId.length > 80)) {
    throw new Error('tenant must be a nonempty identifier of at most 80 characters');
  }
  return { limit, tenantId, filter };
}

async function listShadowEvents(db, options) {
  const { limit, tenantId, filter } = reviewOptions(options);
  const values = [];
  const where = [];
  if (tenantId !== null) { values.push(tenantId); where.push(`tenant_id = $${values.length}`); }
  if (filter === 'mismatches') where.push("comparison = 'mismatch'");
  if (filter === 'errors') where.push("(status = 'error' OR jsonb_array_length(errors) > 0)");
  if (filter === 'warnings') where.push(`status = 'evaluated' AND provider = 'jev' AND comparison = 'mismatch'
    AND current_route->>'failed' IS DISTINCT FROM 'true'
    AND (current_route->>'route' = 'conversation' OR current_route->>'raw_route' = 'intelligence')
    AND (intent = 'status_check' OR recommended_route = 'inspection')
    AND (confidence >= 0.85 OR inspection_probability >= 0.85)`);
  values.push(limit);
  const result = await db.query(`SELECT ${FIELDS.join(', ')} FROM decision_shadow_events
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY timestamp DESC, decision_id DESC LIMIT $${values.length}`, values);
  return result.rows.map(normalizeShadowEvent);
}

module.exports = { FIELDS, insertShadowEvent, listShadowEvents, normalizeShadowEvent, reviewOptions };

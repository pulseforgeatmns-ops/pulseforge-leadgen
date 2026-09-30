'use strict';

const { randomUUID } = require('node:crypto');

const FIELDS = Object.freeze([
  'evidence_id', 'event', 'spec', 'decision_id', 'session_id', 'tenant_id', 'mission_id',
  'pending_decision_kind', 'jev_would_reclassify', 'deterministic_guard_blocked', 'payload', 'timestamp',
]);
const JSON_FIELDS = new Set(['payload']);

const INSERT = `INSERT INTO decision_shadow_evidence (${FIELDS.join(', ')})
  VALUES (${FIELDS.map((_, i) => `$${i + 1}`).join(', ')})`;

function insertShadowEvidence(db, row) {
  const values = FIELDS.map((field) => {
    if (row[field] == null) return null;
    return JSON_FIELDS.has(field) ? JSON.stringify(row[field]) : row[field];
  });
  return db.query(INSERT, values);
}

function normalizeEvidenceRow(row) {
  return {
    ...row,
    timestamp: row.timestamp instanceof Date ? row.timestamp.toISOString() : row.timestamp,
  };
}

async function listShadowEvidence(db, { limit = 100, tenantId = null, event = null } = {}) {
  const values = [];
  const where = [];
  if (tenantId != null) {
    values.push(tenantId);
    where.push(`tenant_id = $${values.length}`);
  }
  if (event) {
    values.push(event);
    where.push(`event = $${values.length}`);
  }
  values.push(Math.min(Math.max(Number(limit) || 100, 1), 500));
  const result = await db.query(`SELECT ${FIELDS.join(', ')} FROM decision_shadow_evidence
    ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
    ORDER BY timestamp DESC, evidence_id DESC LIMIT $${values.length}`, values);
  return result.rows.map(normalizeEvidenceRow);
}

async function countShadowEvidenceSince(db, event, sinceIso) {
  const result = await db.query(
    `SELECT COUNT(*)::int AS n FROM decision_shadow_evidence WHERE event = $1 AND timestamp >= $2`,
    [event, sinceIso],
  );
  return result.rows[0]?.n || 0;
}

function buildGuardEvidenceRow(payload = {}) {
  return {
    evidence_id: randomUUID(),
    event: 'PENDING_DECISION_CAPTURE_GUARDED',
    spec: payload.spec || 'SPEC-JEV-004',
    decision_id: payload.decision_id || null,
    session_id: payload.session_id || payload.sessionId || null,
    tenant_id: payload.tenant_id || payload.tenantId || null,
    mission_id: payload.mission_id || payload.missionId || null,
    pending_decision_kind: payload.pending_decision_kind || payload.pendingDecisionKind || null,
    jev_would_reclassify: Boolean(payload.jev_would_reclassify ?? payload.jevReason),
    deterministic_guard_blocked: payload.deterministic_guard_blocked !== false,
    payload: {
      classification: payload.classification || null,
      reason: payload.reason || null,
      message_chars: payload.message_chars ?? payload.messageChars ?? 0,
    },
    timestamp: new Date().toISOString(),
  };
}

function buildWarningEvidenceRow(warning = {}) {
  return {
    evidence_id: randomUUID(),
    event: 'DECISION_SHADOW_WARNING',
    spec: warning.spec || 'SPEC-JEV-003',
    decision_id: warning.decision_id || null,
    session_id: null,
    tenant_id: warning.tenant_id || null,
    mission_id: warning.mission_id || null,
    pending_decision_kind: warning.current_route?.kind || null,
    jev_would_reclassify: false,
    deterministic_guard_blocked: false,
    payload: warning,
    timestamp: warning.timestamp || new Date().toISOString(),
  };
}

module.exports = {
  FIELDS,
  insertShadowEvidence,
  listShadowEvidence,
  countShadowEvidenceSince,
  buildGuardEvidenceRow,
  buildWarningEvidenceRow,
};

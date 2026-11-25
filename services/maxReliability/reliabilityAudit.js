'use strict';

/**
 * MAX-P0-003 — durable, redacted reliability audit trail. One row per state
 * change of an external action, request receipt or recovery decision. Subject
 * keys are stored as hashes; details pass through redact().
 */

const { redact, shortHash } = require('./redaction');

async function recordReliabilityAudit(pool, entry = {}) {
  await pool.query(
    `INSERT INTO max_reliability_audit
       (tenant_id, correlation_id, subject_kind, subject_key_hash, event, from_state, to_state, detail)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [
      String(entry.tenantId),
      entry.correlationId || null,
      String(entry.subjectKind || 'unknown'),
      entry.subjectKey != null ? shortHash(entry.subjectKey) : null,
      String(entry.event || 'unknown').slice(0, 80),
      entry.fromState || null,
      entry.toState || null,
      redact(entry.detail || {}),
    ]
  );
}

async function listReliabilityAudit(pool, tenantId, { limit = 200, correlationId = null } = {}) {
  const r = await pool.query(
    `SELECT id, tenant_id, correlation_id, subject_kind, subject_key_hash, event, from_state, to_state, detail, at
       FROM max_reliability_audit
      WHERE tenant_id = $1 AND ($2::text IS NULL OR correlation_id = $2)
      ORDER BY id ASC LIMIT $3`,
    [String(tenantId), correlationId, Math.min(Math.max(1, Number(limit) || 200), 2000)]
  );
  return r.rows;
}

module.exports = { recordReliabilityAudit, listReliabilityAudit };

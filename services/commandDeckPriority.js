'use strict';

/**
 * SPEC-097 Living Command Deck — persist domain priority for explainable transitions.
 */

const pool = require('../db');

const VALID_STATES = new Set(['monitored', 'normal', 'elevated', 'urgent']);

let ensured = false;

async function ensureTable() {
  if (ensured) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS command_deck_domain_priority (
      tenant_id TEXT NOT NULL,
      domain_id TEXT NOT NULL,
      priority_state TEXT NOT NULL DEFAULT 'normal',
      previous_state TEXT,
      transition_reason TEXT,
      evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
      changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      reviewed_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (tenant_id, domain_id)
    )
  `);
  ensured = true;
}

function normalizeState(value) {
  const s = String(value || 'normal').toLowerCase();
  return VALID_STATES.has(s) ? s : 'normal';
}

/**
 * @param {string} tenantId
 * @returns {Promise<Map<string, object>>}
 */
async function loadPriorities(tenantId) {
  await ensureTable();
  const result = await pool.query(
    `SELECT * FROM command_deck_domain_priority WHERE tenant_id = $1`,
    [String(tenantId)]
  );
  const map = new Map();
  for (const row of result.rows) {
    map.set(String(row.domain_id), mapRow(row));
  }
  return map;
}

function mapRow(row) {
  if (!row) return null;
  return {
    domainId: String(row.domain_id),
    priorityState: normalizeState(row.priority_state),
    previousState: row.previous_state ? normalizeState(row.previous_state) : null,
    transitionReason: row.transition_reason || null,
    evidenceRefs: Array.isArray(row.evidence_refs)
      ? row.evidence_refs
      : row.evidence_refs || [],
    changedAt: row.changed_at,
    reviewedAt: row.reviewed_at || null,
    updatedAt: row.updated_at,
  };
}

/**
 * Reconcile computed priority with stored state; persist transitions.
 *
 * @param {object} input
 * @param {string} input.tenantId
 * @param {string} input.domainId
 * @param {string} input.computedPriority
 * @param {string|null} [input.reason]
 * @param {object[]} [input.evidenceRefs]
 * @param {Map<string, object>} [input.stored]
 * @returns {Promise<{ priority: string, previousPriority: string|null, transition: object|null }>}
 */
async function reconcileDomainPriority(input) {
  await ensureTable();
  const tenantId = String(input.tenantId);
  const domainId = String(input.domainId);
  const computed = normalizeState(input.computedPriority);
  const stored =
    (input.stored && input.stored.get(domainId)) ||
    mapRow(
      (
        await pool.query(
          `SELECT * FROM command_deck_domain_priority WHERE tenant_id = $1 AND domain_id = $2`,
          [tenantId, domainId]
        )
      ).rows[0]
    );

  const previous = stored ? stored.priorityState : null;
  let transition = null;

  if (!stored || previous !== computed) {
    const reason = input.reason || null;
    const evidenceRefs = input.evidenceRefs || [];
    await pool.query(
      `INSERT INTO command_deck_domain_priority (
        tenant_id, domain_id, priority_state, previous_state,
        transition_reason, evidence_refs, changed_at, updated_at
      ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,NOW(),NOW())
      ON CONFLICT (tenant_id, domain_id) DO UPDATE SET
        previous_state = command_deck_domain_priority.priority_state,
        priority_state = EXCLUDED.priority_state,
        transition_reason = EXCLUDED.transition_reason,
        evidence_refs = EXCLUDED.evidence_refs,
        changed_at = CASE
          WHEN command_deck_domain_priority.priority_state = EXCLUDED.priority_state
          THEN command_deck_domain_priority.changed_at
          ELSE NOW()
        END,
        updated_at = NOW()`,
      [
        tenantId,
        domainId,
        computed,
        previous,
        reason,
        JSON.stringify(evidenceRefs),
      ]
    );

    if (previous && previous !== computed) {
      transition = {
        domain: domainId,
        previousState: previous,
        newState: computed,
        reason: reason || 'Priority changed based on current intelligence.',
        evidenceRefs,
        changedAt: new Date().toISOString(),
        reviewedAt: null,
      };
    }
  }

  return {
    priority: computed,
    previousPriority: previous,
    transition,
  };
}

/**
 * Mark a domain transition as reviewed by the operator.
 * @param {string} tenantId
 * @param {string} domainId
 */
async function markReviewed(tenantId, domainId) {
  await ensureTable();
  await pool.query(
    `UPDATE command_deck_domain_priority
     SET reviewed_at = NOW(), updated_at = NOW()
     WHERE tenant_id = $1 AND domain_id = $2`,
    [String(tenantId), String(domainId)]
  );
}

module.exports = {
  ensureTable,
  loadPriorities,
  reconcileDomainPriority,
  markReviewed,
  normalizeState,
};

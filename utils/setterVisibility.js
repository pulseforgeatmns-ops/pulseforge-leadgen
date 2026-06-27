'use strict';

const SETTER_ICP_THRESHOLD = 70;
const TERMINAL_PROSPECT_STATUSES = new Set(['dead', 'disqualified', 'bounced', 'do_not_email']);
const AUTOMATED_REASONS = new Set(['scout', 'handoff', 'recompute']);
const OVERRIDE_REASONS = new Set(['manual', 'engagement']);
const VALID_REASONS = new Set([...AUTOMATED_REASONS, ...OVERRIDE_REASONS, 'stage_change']);
let visibilitySchemaReady;

function getSetterThreshold(_clientId) {
  return SETTER_ICP_THRESHOLD;
}

function normalizeReason(reason) {
  if (!VALID_REASONS.has(reason)) {
    throw new Error(`Invalid setter visibility reason: ${reason}`);
  }
  return reason;
}

function isHardExcluded(row, { stageStatus } = {}) {
  const status = String(row?.status || '').trim().toLowerCase();
  return TERMINAL_PROSPECT_STATUSES.has(status) ||
    Boolean(row?.do_not_contact) ||
    (stageStatus != null && String(stageStatus).trim().toLowerCase() === 'dead');
}

function passesSoftGate(row) {
  return Number(row?.icp_score || 0) >= getSetterThreshold(row?.client_id) &&
    String(row?.service_area_match || '').trim().length > 0;
}

function computeSetterVisible(row, { reason, stageStatus } = {}) {
  normalizeReason(reason);
  if (isHardExcluded(row, { stageStatus })) return false;
  if (AUTOMATED_REASONS.has(reason)) return passesSoftGate(row);
  if (OVERRIDE_REASONS.has(reason)) return true;
  // Stage changes do not create a new override. Non-dead changes retain the
  // lead's current visibility; moving to dead is handled above.
  return Boolean(row?.setter_visible);
}

async function ensureSetterVisibilitySchema(db) {
  if (!visibilitySchemaReady) {
    visibilitySchemaReady = db.query(`
      ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS setter_visibility_reason TEXT
    `).catch(err => {
      visibilitySchemaReady = null;
      throw err;
    });
  }
  return visibilitySchemaReady;
}

async function setSetterVisibility(db, prospectId, {
  reason,
  clientId = null,
  source = null,
  stageStatus = null,
} = {}) {
  normalizeReason(reason);
  await ensureSetterVisibilitySchema(db);

  const params = [prospectId];
  let where = 'id = $1';
  if (clientId != null) {
    params.push(clientId);
    where += ` AND client_id = $${params.length}`;
  }
  if (source != null) {
    params.push(source);
    where += ` AND source = $${params.length}`;
  }

  const current = await db.query(`
    SELECT id, client_id, status, do_not_contact, icp_score,
           service_area_match, setter_visible, setter_visibility_reason
    FROM prospects
    WHERE ${where}
    FOR UPDATE
  `, params);
  if (!current.rows.length) return null;

  const row = current.rows[0];
  const visible = computeSetterVisible(row, { reason, stageStatus });
  const hardExcluded = isHardExcluded(row, { stageStatus });
  const storedReason = hardExcluded
    ? 'hard_exclusion'
    : reason === 'stage_change'
      ? (row.setter_visibility_reason || 'stage_change')
      : reason;

  const updated = await db.query(`
    UPDATE prospects
    SET setter_visible = $2,
        setter_visibility_reason = $3,
        setter_updated_at = CASE
          WHEN COALESCE(setter_visible, false) IS DISTINCT FROM $2 THEN NOW()
          ELSE setter_updated_at
        END
    WHERE id = $1
    RETURNING *
  `, [prospectId, visible, storedReason]);
  return updated.rows[0] || null;
}

async function recomputeSetterVisibility(db, {
  clientId = null,
  reason = 'recompute',
  preserveOverrides = true,
} = {}) {
  normalizeReason(reason);
  if (!AUTOMATED_REASONS.has(reason)) {
    throw new Error(`Bulk recompute requires an automated reason, received: ${reason}`);
  }
  await ensureSetterVisibilitySchema(db);

  const result = await db.query(`
    WITH decisions AS (
      SELECT
        id,
        COALESCE(setter_visible, false) AS old_visible,
        setter_visibility_reason AS old_reason,
        CASE
          WHEN COALESCE(status, '') IN ('dead', 'disqualified', 'bounced', 'do_not_email')
            OR COALESCE(do_not_contact, false) = true
            THEN false
          WHEN $2::boolean = true
            AND COALESCE(setter_visible, false) = true
            AND COALESCE(setter_visibility_reason, '') IN ('manual', 'engagement')
            THEN true
          ELSE COALESCE(icp_score, 0) >= $3
            AND NULLIF(BTRIM(COALESCE(service_area_match, '')), '') IS NOT NULL
        END AS next_visible,
        CASE
          WHEN COALESCE(status, '') IN ('dead', 'disqualified', 'bounced', 'do_not_email')
            OR COALESCE(do_not_contact, false) = true
            THEN 'hard_exclusion'
          WHEN $2::boolean = true
            AND COALESCE(setter_visible, false) = true
            AND COALESCE(setter_visibility_reason, '') IN ('manual', 'engagement')
            THEN setter_visibility_reason
          ELSE $4::text
        END AS next_reason
      FROM prospects
      WHERE ($1::int IS NULL OR client_id = $1)
    ), updated AS (
      UPDATE prospects p
      SET setter_visible = d.next_visible,
          setter_visibility_reason = d.next_reason,
          setter_updated_at = CASE
            WHEN COALESCE(p.setter_visible, false) IS DISTINCT FROM d.next_visible THEN NOW()
            ELSE p.setter_updated_at
          END
      FROM decisions d
      WHERE p.id = d.id
      RETURNING p.id, p.client_id, p.status, p.setter_visible, d.old_visible
    )
    SELECT id, client_id, status, setter_visible, old_visible
    FROM updated
  `, [clientId, preserveOverrides, SETTER_ICP_THRESHOLD, reason]);

  return {
    rows: result.rows,
    evaluated: result.rowCount,
    promoted: result.rows.filter(row => !row.old_visible && row.setter_visible).length,
    demoted: result.rows.filter(row => row.old_visible && !row.setter_visible).length,
  };
}

module.exports = {
  SETTER_ICP_THRESHOLD,
  TERMINAL_PROSPECT_STATUSES: [...TERMINAL_PROSPECT_STATUSES],
  computeSetterVisible,
  ensureSetterVisibilitySchema,
  getSetterThreshold,
  isHardExcluded,
  passesSoftGate,
  recomputeSetterVisibility,
  setSetterVisibility,
};

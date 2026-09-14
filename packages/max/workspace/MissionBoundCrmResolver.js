'use strict';

/**
 * PREPARE-time projection of verified CRM recipient email onto mission-bound candidates.
 * Never expands the candidate universe; never infers addresses.
 */

const { invalidOutreachEmailReason } = require('../../../utils/emailGuard');
const { canonicalOutboundEmailIneligibilityReason } = require('../../../utils/canonicalEmailEligibility');

const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function normalizeClientId(value) {
  const n = Number(value);
  return Number.isInteger(n) && n > 0 ? n : null;
}

function crmLookup(crmByProspectId, prospectId) {
  if (!crmByProspectId || prospectId == null) return null;
  if (crmByProspectId instanceof Map) {
    return crmByProspectId.get(String(prospectId)) || null;
  }
  if (typeof crmByProspectId === 'object') {
    return crmByProspectId[String(prospectId)] || crmByProspectId[prospectId] || null;
  }
  return null;
}

/**
 * True when a canonical CRM prospect row may be used for outbound recipient projection.
 * @param {object|null} row
 * @returns {boolean}
 */
function isProjectableCrmProspect(row) {
  if (!row || typeof row !== 'object') return false;
  if (row.do_not_contact === true) return false;
  const email = String(row.email || '').trim();
  if (!email || invalidOutreachEmailReason(email)) return false;
  if (row.email_verified !== true) return false;
  const status = String(row.email_status || '').toLowerCase();
  if (!VERIFIED_EMAIL_STATUSES.has(status)) return false;
  if (canonicalOutboundEmailIneligibilityReason(row)) return false;
  return true;
}

/**
 * @param {object|null} row
 * @returns {string|null}
 */
function projectableEmailFromCrmRecord(row) {
  if (!isProjectableCrmProspect(row)) return null;
  return String(row.email).trim();
}

/**
 * Resolve mission-bound recipient email: frozen discovery first, then verified CRM.
 * @param {object} input
 * @returns {string|null}
 */
function resolveMissionBoundRecipientEmail({
  discoveryEmail,
  prospectId,
  crmByProspectId,
} = {}) {
  const fromDiscovery = String(discoveryEmail || '').trim();
  if (fromDiscovery) return fromDiscovery;
  return projectableEmailFromCrmRecord(crmLookup(crmByProspectId, prospectId));
}

/**
 * Normalize mission-bound prospect IDs for CRM queries.
 * Production prospects.id is UUID — never coerce through Number().
 * @param {Array<unknown>} raw
 * @returns {string[]}
 */
function normalizeProspectIds(raw = []) {
  return [...new Set(
    (Array.isArray(raw) ? raw : [])
      .map((id) => String(id ?? '').trim())
      .filter((id) => UUID_RE.test(id))
      .map((id) => id.toLowerCase())
  )];
}

/**
 * Batch-load canonical CRM rows for mission-bound prospect IDs (tenant scoped).
 * @param {object} input
 * @returns {Promise<Map<string, object>>}
 */
async function loadCrmProspectsByIds(input = {}) {
  const clientId = normalizeClientId(input.clientId);
  const pool = input.pool;
  const ids = normalizeProspectIds(input.prospectIds);
  const map = new Map();
  if (!clientId || !pool || !ids.length) return map;

  const { rows } = await pool.query(
    `SELECT id, email, email_status, email_verified, do_not_contact
       FROM prospects
      WHERE client_id = $1
        AND id = ANY($2::uuid[])`,
    [clientId, ids]
  );
  for (const row of rows) {
    map.set(String(row.id), row);
  }
  return map;
}

module.exports = {
  VERIFIED_EMAIL_STATUSES,
  normalizeProspectIds,
  isProjectableCrmProspect,
  projectableEmailFromCrmRecord,
  resolveMissionBoundRecipientEmail,
  loadCrmProspectsByIds,
};

'use strict';

/**
 * PREPARE-time projection of verified CRM recipient email onto mission-bound candidates.
 * Never expands the candidate universe; never infers addresses.
 */

const { invalidOutreachEmailReason } = require('../../../utils/emailGuard');

const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);

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
 * Batch-load canonical CRM rows for mission-bound prospect IDs (tenant scoped).
 * @param {object} input
 * @returns {Promise<Map<string, object>>}
 */
async function loadCrmProspectsByIds(input = {}) {
  const clientId = normalizeClientId(input.clientId);
  const pool = input.pool;
  const ids = [...new Set(
    (Array.isArray(input.prospectIds) ? input.prospectIds : [])
      .map((id) => Number(id))
      .filter((id) => Number.isInteger(id) && id > 0)
  )];
  const map = new Map();
  if (!clientId || !pool || !ids.length) return map;

  const { rows } = await pool.query(
    `SELECT id, email, email_status, email_verified, do_not_contact
       FROM prospects
      WHERE client_id = $1
        AND id = ANY($2::int[])`,
    [clientId, ids]
  );
  for (const row of rows) {
    map.set(String(row.id), row);
  }
  return map;
}

const CRM_ENRICHMENT_PROSPECT_SELECT = `
       p.id AS prospect_id,
       p.company_id,
       p.client_id,
       p.first_name,
       p.last_name,
       p.email,
       p.email_status,
       p.email_verified,
       p.email_verification_method,
       p.do_not_contact,
       p.notes,
       p.vertical,
       p.website_url,
       p.employee_count_estimate,
       p.practice_area,
       p.firm_size,
       p.enrichment_provenance,
       c.name AS company_name,
       c.website,
       c.domain,
       c.industry,
       c.size AS company_size,
       c.location,
       c.practice_area AS company_practice_area,
       c.firm_size AS company_firm_size`;

/**
 * Resolve the best CRM contact row for one mission-bound key.
 * Keys are company/candidate IDs from Max prioritization; falls back to prospects.id
 * when the key already matches a contact row (legacy fixtures / integer IDs).
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function loadBestCrmProspectForMissionBoundKey(input = {}) {
  const clientId = normalizeClientId(input.clientId);
  const pool = input.pool;
  const missionBoundKey = String(input.missionBoundKey || input.companyId || '').trim();
  if (!clientId || !pool || !missionBoundKey) return null;

  const { rows } = await pool.query(
    `SELECT
${CRM_ENRICHMENT_PROSPECT_SELECT}
     FROM prospects p
     LEFT JOIN companies c
       ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND (
        p.company_id::text = $2
        OR p.id::text = $2
      )
      AND COALESCE(p.is_synthetic, false) = false
    ORDER BY
      CASE WHEN p.company_id::text = $2 THEN 0 ELSE 1 END,
      p.icp_score DESC NULLS LAST,
      p.created_at ASC,
      p.id ASC
    LIMIT 1`,
    [clientId, missionBoundKey]
  );
  return rows[0] || null;
}

/**
 * Batch-resolve mission-bound company/candidate keys to canonical CRM contact rows.
 * Preserves the input key universe — never adds companies outside the supplied list.
 * @param {object} input
 * @returns {Promise<Map<string, object>>}
 */
async function loadCrmProspectsForMissionBoundCompanies(input = {}) {
  const clientId = normalizeClientId(input.clientId);
  const pool = input.pool;
  const companyIds = [...new Set(
    (Array.isArray(input.companyIds) ? input.companyIds : [])
      .map((id) => String(id || '').trim())
      .filter(Boolean)
  )];
  const map = new Map();
  if (!clientId || !pool || !companyIds.length) return map;

  const { rows } = await pool.query(
    `WITH mission_keys AS (
       SELECT unnest($2::text[]) AS mission_bound_key
     )
     SELECT DISTINCT ON (k.mission_bound_key)
       k.mission_bound_key,
${CRM_ENRICHMENT_PROSPECT_SELECT}
     FROM mission_keys k
     JOIN prospects p
       ON p.client_id = $1
      AND (
        p.company_id::text = k.mission_bound_key
        OR p.id::text = k.mission_bound_key
      )
     LEFT JOIN companies c
       ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE COALESCE(p.is_synthetic, false) = false
    ORDER BY
      k.mission_bound_key,
      CASE WHEN p.company_id::text = k.mission_bound_key THEN 0 ELSE 1 END,
      p.icp_score DESC NULLS LAST,
      p.created_at ASC,
      p.id ASC`,
    [clientId, companyIds]
  );

  for (const row of rows) {
    map.set(String(row.mission_bound_key), row);
  }
  return map;
}

module.exports = {
  VERIFIED_EMAIL_STATUSES,
  isProjectableCrmProspect,
  projectableEmailFromCrmRecord,
  resolveMissionBoundRecipientEmail,
  loadCrmProspectsByIds,
  loadBestCrmProspectForMissionBoundKey,
  loadCrmProspectsForMissionBoundCompanies,
};

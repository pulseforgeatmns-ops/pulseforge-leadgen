'use strict';

/**
 * PREPARE-time projection of verified CRM recipient email onto mission-bound candidates.
 * Never expands the candidate universe; never infers addresses.
 */

const { invalidOutreachEmailReason } = require('../../../utils/emailGuard');
const {
  canonicalOutboundEmailIneligibilityReason,
  resolveEmailProvenanceSource,
} = require('../../../utils/canonicalEmailEligibility');

const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const EXCLUDED_COMPANY_RE = /deliverability\s*test/i;

function isExcludedDeliverabilityTestCompany(name) {
  return EXCLUDED_COMPANY_RE.test(String(name || '').trim());
}

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
  missionBoundKey,
  companyId,
  domain,
  crmByProspectId,
} = {}) {
  const fromDiscovery = String(discoveryEmail || '').trim();
  if (fromDiscovery) return fromDiscovery;
  const keys = [missionBoundKey, prospectId, companyId, domain].filter((key) => key != null && String(key).trim());
  for (const key of keys) {
    const email = projectableEmailFromCrmRecord(crmLookup(crmByProspectId, key));
    if (email) return email;
  }
  return null;
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
    `SELECT id, email, email_status, email_verified, do_not_contact,
            enrichment_provenance
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
        OR (
          c.domain IS NOT NULL
          AND lower(c.domain) = lower($2)
        )
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
      AND COALESCE(p.is_synthetic, false) = false
     LEFT JOIN companies c
       ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE
      p.company_id::text = k.mission_bound_key
      OR p.id::text = k.mission_bound_key
      OR (
        c.domain IS NOT NULL
        AND lower(c.domain) = lower(k.mission_bound_key)
      )
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

/**
 * Read-only CRM inspection for one mission-bound queue key (company/candidate ID).
 * Never infers addresses; preserves provenance and safety gates.
 */
async function inspectMissionBoundCrmForQueueItem(input = {}) {
  const clientId = normalizeClientId(input.clientId);
  const pool = input.pool;
  const missionBoundKey = String(input.missionBoundKey || '').trim();
  const companyName = input.companyName || null;

  const base = {
    missionBoundKey: missionBoundKey || null,
    crmContactId: null,
    crmCompanyId: null,
    crmEmailPresent: false,
    crmProjectable: false,
    crmEmailVerified: false,
    crmEmailStatus: null,
    projectionBlockReason: missionBoundKey ? 'no_crm_contact' : 'missing_mission_bound_key',
    emailProvenanceSource: null,
    deliverabilityTestExcluded: false,
  };

  if (isExcludedDeliverabilityTestCompany(companyName)) {
    return {
      ...base,
      projectionBlockReason: 'deliverability_test_excluded',
      deliverabilityTestExcluded: true,
    };
  }

  if (!clientId || !pool || !missionBoundKey) return base;

  const crm = await loadBestCrmProspectForMissionBoundKey({
    pool,
    clientId,
    missionBoundKey,
  });
  if (!crm) return base;

  const crmEmail = crm.email ? String(crm.email).trim() : null;
  const provenance = resolveEmailProvenanceSource(crm);
  const ineligibility = canonicalOutboundEmailIneligibilityReason(crm);
  const projectable = isProjectableCrmProspect(crm);

  return {
    missionBoundKey,
    crmContactId: crm.prospect_id || crm.id || null,
    crmCompanyId: crm.company_id != null ? String(crm.company_id) : null,
    crmEmailPresent: Boolean(crmEmail),
    crmProjectable: projectable,
    crmEmailVerified: projectable,
    crmEmailStatus: crm.email_status || null,
    projectionBlockReason: projectable ? null : (ineligibility || 'not_projectable'),
    emailProvenanceSource: provenance || null,
    deliverabilityTestExcluded: false,
  };
}

module.exports = {
  VERIFIED_EMAIL_STATUSES,
  EXCLUDED_COMPANY_RE,
  normalizeProspectIds,
  isExcludedDeliverabilityTestCompany,
  isProjectableCrmProspect,
  projectableEmailFromCrmRecord,
  resolveMissionBoundRecipientEmail,
  inspectMissionBoundCrmForQueueItem,
  loadCrmProspectsByIds,
  loadBestCrmProspectForMissionBoundKey,
  loadCrmProspectsForMissionBoundCompanies,
};

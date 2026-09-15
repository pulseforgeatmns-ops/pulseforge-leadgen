'use strict';

/**
 * Canonical CRM admission for mission-bound Scout candidates before contact enrichment.
 * Deterministic identity only — no fuzzy company-name merge.
 *
 * Lifecycle placement: after Max prioritization, before mission-bound enrichment.
 * Scout/Max/Paige/Emmett PREPARE remain read-only; admission runs in the enrichment runner.
 */

const {
  isGooglePlaceId,
  isUuid,
  normalizeDomain,
  prospectIdentity,
  identityKeysFrom,
} = require('./CanonicalOutboundIdentity');
const { ensureMissionBoundCrmSchema } = require('../../../utils/missionBoundCrmSchema');
const { deriveBusinessNameShort, ensureBusinessNameShortColumns } = require('../../../utils/businessNameShort');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
} = require('./EmmettMissionCandidates');

const ADMISSION_SOURCE = 'mission_bound_admission';

function admissionProvenance(candidate, missionId) {
  return {
    mission_bound_admission: {
      source: 'scout',
      mission_id: missionId,
      candidate_id: candidate.candidateId || candidate.id || null,
      place_id: candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null),
      company_name: candidate.company || null,
      domain: candidate.domain || null,
      admitted_at: new Date().toISOString(),
    },
  };
}

function hasSufficientAdmissionIdentity(candidate = {}) {
  const placeId = candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null);
  const domain = normalizeDomain(candidate.domain);
  const companyUuid = isUuid(candidate.companyId) ? String(candidate.companyId) : null;
  const prospectUuid = candidate.crmProspectId && isUuid(candidate.crmProspectId)
    ? String(candidate.crmProspectId)
    : null;
  return Boolean(placeId || domain || companyUuid || prospectUuid);
}

/**
 * Resolve existing tenant-scoped company by deterministic keys (priority order).
 * @returns {Promise<{ company: object, matchType: string }|null>}
 */
async function resolveExistingCompany(pool, clientId, candidate = {}) {
  const placeId = candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null);
  const domain = normalizeDomain(candidate.domain);
  const companyUuid = isUuid(candidate.companyId) ? String(candidate.companyId) : null;

  const hits = [];

  if (placeId) {
    const { rows } = await pool.query(
      `SELECT id, name, domain, website, google_place_id, client_id, enrichment_provenance
         FROM companies
        WHERE client_id = $1
          AND google_place_id = $2
        LIMIT 1`,
      [clientId, placeId]
    );
    if (rows[0]) hits.push({ company: rows[0], matchType: 'google_place_id' });
  }

  if (domain) {
    const { rows } = await pool.query(
      `SELECT id, name, domain, website, google_place_id, client_id, enrichment_provenance
         FROM companies
        WHERE client_id = $1
          AND domain IS NOT NULL
          AND lower(domain) = lower($2)
        LIMIT 1`,
      [clientId, domain]
    );
    if (rows[0]) hits.push({ company: rows[0], matchType: 'domain' });
  }

  if (companyUuid) {
    const { rows } = await pool.query(
      `SELECT id, name, domain, website, google_place_id, client_id, enrichment_provenance
         FROM companies
        WHERE client_id = $1
          AND id::text = $2
        LIMIT 1`,
      [clientId, companyUuid]
    );
    if (rows[0]) hits.push({ company: rows[0], matchType: 'company_uuid' });
  }

  if (!hits.length) return null;

  const uniqueIds = [...new Set(hits.map((row) => String(row.company.id)))];
  if (uniqueIds.length > 1) {
    return {
      blocked: true,
      reason: 'identity_admission_blocked',
      detail: 'conflicting_company_identity',
      matches: hits.map((row) => ({ id: row.company.id, matchType: row.matchType })),
    };
  }

  return hits[0];
}

async function resolveExistingProspect(pool, clientId, companyId, candidate = {}) {
  const crmProspectId = candidate.crmProspectId && isUuid(candidate.crmProspectId)
    ? String(candidate.crmProspectId)
    : null;

  if (crmProspectId) {
    const { rows } = await pool.query(
      `SELECT id, company_id, client_id, email, email_verified, email_status, do_not_contact
         FROM prospects
        WHERE client_id = $1
          AND id = $2::uuid
          AND COALESCE(is_synthetic, false) = false
        LIMIT 1`,
      [clientId, crmProspectId]
    );
    if (rows[0]) return { prospect: rows[0], matchType: 'prospect_uuid' };
  }

  const { rows } = await pool.query(
    `SELECT id, company_id, client_id, email, email_verified, email_status, do_not_contact
       FROM prospects
      WHERE client_id = $1
        AND company_id = $2::uuid
        AND COALESCE(is_synthetic, false) = false
      ORDER BY icp_score DESC NULLS LAST, created_at ASC, id ASC
      LIMIT 1`,
    [clientId, companyId]
  );
  if (rows[0]) return { prospect: rows[0], matchType: 'company_prospect' };
  return null;
}

async function linkCompanyExternalIdentity(pool, company, candidate, missionId, clientId, dryRun) {
  const placeId = candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null);
  const domain = normalizeDomain(candidate.domain);
  const website = candidate.website || candidate.website_url || null;
  const provenance = admissionProvenance(candidate, missionId);

  if (dryRun) {
    return {
      ...company,
      google_place_id: placeId || company.google_place_id,
      domain: company.domain || domain,
    };
  }

  const { rows } = await pool.query(
    `UPDATE companies
        SET google_place_id = COALESCE(google_place_id, $1),
            domain = COALESCE(domain, $2),
            website = COALESCE(website, $3),
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $4::jsonb,
            updated_at = NOW()
      WHERE id = $5
        AND client_id = $6
      RETURNING id, name, domain, website, google_place_id, client_id, enrichment_provenance`,
    [
      placeId,
      domain,
      website,
      JSON.stringify(provenance),
      company.id,
      clientId,
    ]
  );
  return rows[0] || company;
}

async function createCompanyFromCandidate(pool, candidate, missionId, clientId, dryRun) {
  const name = String(candidate.company || '').trim();
  const placeId = candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null);
  const domain = normalizeDomain(candidate.domain);
  const website = candidate.website || candidate.website_url || null;

  if (!name) {
    return { blocked: true, reason: 'identity_admission_blocked', detail: 'missing_company_name' };
  }
  if (!placeId && !domain) {
    return { blocked: true, reason: 'identity_admission_blocked', detail: 'insufficient_company_identity' };
  }

  if (dryRun) {
    return {
      company: {
        id: 'dry-run-company-id',
        name,
        domain,
        website,
        google_place_id: placeId,
        client_id: clientId,
      },
      created: true,
      matchType: 'created',
    };
  }

  await ensureBusinessNameShortColumns(pool);
  const shortName = deriveBusinessNameShort(name);
  const provenance = admissionProvenance(candidate, missionId);

  const { rows } = await pool.query(
    `INSERT INTO companies (
       name, business_name_short, business_name_short_confidence, business_name_short_flags,
       domain, website, google_place_id, industry, location, client_id,
       enrichment_provenance, created_at
     )
     VALUES ($1, $2, $3, $4::text[], $5, $6, $7, $8, $9, $10, $11::jsonb, NOW())
     RETURNING id, name, domain, website, google_place_id, client_id, enrichment_provenance`,
    [
      name,
      shortName.business_name_short,
      shortName.confidence,
      shortName.flags,
      domain,
      website,
      placeId,
      candidate.vertical || null,
      candidate.location || null,
      clientId,
      JSON.stringify(provenance),
    ]
  );

  return { company: rows[0], created: true, matchType: 'created' };
}

async function createProspectForCompany(pool, companyId, candidate, clientId, dryRun) {
  if (dryRun) {
    return {
      prospect: {
        id: 'dry-run-prospect-id',
        company_id: companyId,
        client_id: clientId,
      },
      created: true,
      matchType: 'created',
    };
  }

  const { rows } = await pool.query(
    `INSERT INTO prospects (
       company_id, first_name, last_name, email, status, source, icp_score,
       vertical, client_id, website_url, discovery_method, has_website
     )
     VALUES ($1, NULL, NULL, NULL, 'cold', $2, $3, $4, $5, $6, $7, $8)
     RETURNING id, company_id, client_id, email, email_verified, email_status, do_not_contact`,
    [
      companyId,
      ADMISSION_SOURCE,
      candidate.icpScore || 0,
      candidate.vertical || null,
      clientId,
      candidate.website || candidate.website_url || null,
      'mission_bound',
      Boolean(normalizeDomain(candidate.domain) || candidate.website),
    ]
  );

  return { prospect: rows[0], created: true, matchType: 'created' };
}

async function logAdmission(pool, candidate, result, clientId, dryRun) {
  if (dryRun || !result.prospect?.id) return;
  await pool.query(
    `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW(), $6)`,
    [
      'mission_bound_crm_admission',
      result.created ? 'admitted' : 'linked',
      result.prospect.id,
      JSON.stringify({
        candidateId: candidate.candidateId || candidate.id,
        placeId: candidate.placeId || null,
        companyId: result.company?.id || null,
        matchType: result.matchType,
        reason: result.reason || null,
      }),
      result.blocked ? 'blocked' : 'success',
      clientId,
    ]
  );
}

/**
 * Admit or link one mission-bound candidate into tenant CRM.
 * @returns {Promise<object>}
 */
async function admitMissionBoundCandidate(pool, candidate, {
  missionId,
  clientId,
  dryRun = false,
} = {}) {
  const base = {
    candidateId: candidate.candidateId || candidate.id,
    placeId: candidate.placeId || (isGooglePlaceId(candidate.id) ? candidate.id : null),
    domain: normalizeDomain(candidate.domain),
    companyName: candidate.company || null,
    companyId: null,
    crmCompanyId: null,
    prospectId: null,
    crmProspectId: null,
    linked: false,
    created: false,
    blocked: false,
    reason: null,
    matchType: null,
  };

  if (!hasSufficientAdmissionIdentity(candidate)) {
    return {
      ...base,
      blocked: true,
      reason: 'identity_admission_blocked',
      detail: 'insufficient_identity',
    };
  }

  let companyResult = await resolveExistingCompany(pool, clientId, candidate);
  if (companyResult?.blocked) {
    return { ...base, ...companyResult };
  }

  let company = companyResult?.company || null;
  let matchType = companyResult?.matchType || null;
  let created = false;

  if (company) {
    company = await linkCompanyExternalIdentity(pool, company, candidate, missionId, clientId, dryRun);
  } else {
    const createdResult = await createCompanyFromCandidate(pool, candidate, missionId, clientId, dryRun);
    if (createdResult.blocked) return { ...base, ...createdResult };
    company = createdResult.company;
    matchType = createdResult.matchType;
    created = true;
  }

  let prospectResult = await resolveExistingProspect(pool, clientId, company.id, candidate);
  let prospect = prospectResult?.prospect || null;
  let prospectCreated = false;

  if (!prospect) {
    const createdProspect = await createProspectForCompany(pool, company.id, candidate, clientId, dryRun);
    prospect = createdProspect.prospect;
    prospectCreated = true;
    if (!matchType || matchType === 'created') {
      matchType = created ? 'created' : `${matchType}+prospect_created`;
    }
  }

  const result = {
    ...base,
    companyId: String(company.id),
    crmCompanyId: String(company.id),
    prospectId: String(prospect.id),
    crmProspectId: String(prospect.id),
    domain: normalizeDomain(company.domain || candidate.domain),
    linked: !created && !prospectCreated,
    created: created || prospectCreated,
    blocked: false,
    reason: created || prospectCreated ? 'admitted' : 'linked_existing',
    matchType,
    company,
    prospect,
  };

  await logAdmission(pool, candidate, result, clientId, dryRun);
  return result;
}

/**
 * Admit all prioritized mission-bound candidates for one mission.
 * @returns {Promise<{ results: object[], byCandidateId: Map<string, object> }>}
 */
async function admitMissionBoundCandidates(pool, mission, contributions = [], opts = {}) {
  const clientId = Number(opts.clientId || mission.clientId || mission.client_id);
  const missionId = mission.id || opts.missionId;
  const dryRun = Boolean(opts.dryRun);
  const candidateFilter = opts.candidateIds
    ? new Set((opts.candidateIds || []).map(String))
    : null;

  await ensureMissionBoundCrmSchema(pool);

  const candidates = buildMissionBoundCandidates(mission, contributions);
  const prioritizedIds = new Set(listMissionBoundCompanyIds(mission, contributions).map(String));
  const targets = candidates.filter((row) => {
    const id = String(row.id);
    if (!prioritizedIds.has(id)) return false;
    if (candidateFilter && !candidateFilter.has(id)) return false;
    return true;
  });

  const results = [];
  const byCandidateId = new Map();

  for (const candidate of targets) {
    const result = await admitMissionBoundCandidate(pool, candidate, {
      missionId,
      clientId,
      dryRun,
    });
    results.push(result);
    for (const key of identityKeysFrom(candidate)) {
      byCandidateId.set(String(key), result);
    }
  }

  return { results, byCandidateId, candidates, targets };
}

module.exports = {
  ADMISSION_SOURCE,
  admissionProvenance,
  hasSufficientAdmissionIdentity,
  resolveExistingCompany,
  resolveExistingProspect,
  admitMissionBoundCandidate,
  admitMissionBoundCandidates,
  ensureMissionBoundCrmSchema,
};

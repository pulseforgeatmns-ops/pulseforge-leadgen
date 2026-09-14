'use strict';

/**
 * Shared enrichment helpers for Anchor mission-bound contacts.
 * Never sends mail. Never revises CAPACITY.
 */

const { getAcquisitionMissionRuntime } = require('../../services/acquisitionMissionRuntime');
const { listMissionBoundCompanyIds } = require('../../packages/max/workspace/EmmettMissionCandidates');
const {
  isProjectableCrmProspect,
  loadCrmProspectsForMissionBoundCompanies,
} = require('../../packages/max/workspace/MissionBoundCrmResolver');
const {
  configureScoringContext,
  runEnrichmentChain,
  resolveEmailVerification,
  normalizeDomain,
} = require('../../leadgen');
const tiered = require('../../tieredEnrichmentAgent');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const DEFAULT_MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const EXCLUDED_COMPANY_RE = /deliverability\s*test/i;

function isExcludedCompany(name) {
  return EXCLUDED_COMPANY_RE.test(String(name || '').trim());
}

function crmProjectionRow(row = {}) {
  return {
    email: row.email,
    email_verified: row.email_verified,
    email_status: row.email_status,
    do_not_contact: row.do_not_contact,
  };
}

async function loadProspectRow(db, clientId, prospectId) {
  const { loadBestCrmProspectForMissionBoundKey } = require('../../packages/max/workspace/MissionBoundCrmResolver');
  return loadBestCrmProspectForMissionBoundKey({
    pool: db,
    clientId,
    missionBoundKey: prospectId,
  });
}

async function loadMissionBoundProspects(db, missionId) {
  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    throw Object.assign(new Error(`Mission ${missionId} not found.`), { code: 'mission_not_found' });
  }
  const snapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const companyIds = listMissionBoundCompanyIds(mission, snapshot.contributions || []);
  const rowsByCompanyId = await loadCrmProspectsForMissionBoundCompanies({
    pool: db,
    clientId: CLIENT_ID,
    companyIds,
  });
  const rows = companyIds
    .map((companyId) => rowsByCompanyId.get(String(companyId)))
    .filter(Boolean);
  return {
    mission,
    snapshot,
    companyIds,
    // Legacy report field: mission-bound keys (company/candidate IDs), not CRM prospects.id.
    prospectIds: companyIds,
    rowsByCompanyId,
    rows,
  };
}

async function persistProviderChainEmail(db, row, enriched, verification, dryRun) {
  const candidate = {
    email: enriched.email,
    email_verified: verification.emailVerified === true,
    email_status: verification.emailStatus,
    do_not_contact: verification.doNotContact === true,
  };
  if (!isProjectableCrmProspect(candidate)) {
    return { persisted: false, reason: 'failed_safety_gates', candidate };
  }
  if (dryRun) {
    return { persisted: false, dryRun: true, wouldPersist: candidate.email };
  }

  const contactParts = String(enriched.contact || '').trim().split(/\s+/).filter(Boolean);
  const firstName = contactParts[0] || null;
  const lastName = contactParts.length > 1 ? contactParts.slice(1).join(' ') : null;

  await db.query(
    `UPDATE prospects
        SET email = $1,
            first_name = COALESCE(NULLIF(TRIM(first_name), ''), $2),
            last_name = COALESCE(NULLIF(TRIM(last_name), ''), $3),
            email_verified = $4,
            email_verification_method = $5,
            email_status = $6,
            verified_at = COALESCE(verified_at, NOW()),
            verifier_checked_at = NOW(),
            verifier_response = $7::jsonb,
            do_not_contact = CASE WHEN $8 THEN true ELSE do_not_contact END,
            updated_at = NOW()
      WHERE id = $9
        AND client_id = $10`,
    [
      candidate.email,
      firstName,
      lastName,
      true,
      verification.emailVerificationMethod,
      verification.emailStatus,
      JSON.stringify(verification.verifierResponse || null),
      verification.doNotContact === true,
      row.prospect_id,
      row.client_id,
    ]
  );

  await db.query(
    `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW(), $6)`,
    [
      'mission_bound_enrichment',
      'provider_chain_persist',
      row.prospect_id,
      JSON.stringify({
        source: enriched.source || null,
        email: candidate.email,
        email_status: verification.emailStatus,
        email_verification_method: verification.emailVerificationMethod,
      }),
      'success',
      row.client_id,
    ]
  );

  return { persisted: true, email: candidate.email };
}

async function enrichProspectRow(row, options = {}) {
  const company = row.company_name || row.company || null;
  const base = {
    prospectId: String(row.prospect_id),
    missionBoundCompanyId: row.company_id != null ? String(row.company_id) : null,
    company,
    excluded: false,
    verified: false,
    persisted: false,
    path: null,
    email: null,
    emailStatus: row.email_status || null,
    emailVerificationMethod: row.email_verification_method || null,
    verificationSource: null,
    dnc: row.do_not_contact === true,
    reason: null,
  };

  if (isExcludedCompany(company)) {
    return {
      ...base,
      excluded: true,
      reason: 'deliverability_test_excluded',
    };
  }

  if (isProjectableCrmProspect(crmProjectionRow(row))) {
    return {
      ...base,
      verified: true,
      persisted: true,
      path: 'existing_crm',
      email: String(row.email).trim(),
      emailStatus: row.email_status,
      emailVerificationMethod: row.email_verification_method,
      verificationSource: 'existing_crm',
      reason: 'already_projectable',
    };
  }

  const tieredOutcome = await tiered._test.processProspect(row, {
    dryRun: options.dryRun,
    fetchDelayMs: options.fetchDelayMs,
  });

  const reloaded = options.dryRun
    ? row
    : await loadProspectRow(options.db, row.client_id, row.prospect_id);

  if (reloaded && isProjectableCrmProspect(crmProjectionRow(reloaded))) {
    return {
      ...base,
      verified: true,
      persisted: !options.dryRun,
      path: 'tiered_enrichment',
      email: String(reloaded.email).trim(),
      emailStatus: reloaded.email_status,
      emailVerificationMethod: reloaded.email_verification_method,
      verificationSource: tieredOutcome.selectedEmail?.source || 'tiered_enrichment',
      reason: tieredOutcome.resolved ? 'tiered_resolved' : 'tiered_email_persisted',
      tiered: {
        resolved: tieredOutcome.resolved,
        resolvedTier: tieredOutcome.resolvedTier,
        errors: tieredOutcome.errors,
      },
    };
  }

  const domain = normalizeDomain(row.domain || row.website || row.website_url);
  if (!domain) {
    return {
      ...base,
      path: 'provider_chain',
      reason: 'no_domain',
      tiered: { errors: tieredOutcome.errors },
    };
  }

  await configureScoringContext({ client_id: row.client_id || CLIENT_ID });
  const enriched = await runEnrichmentChain(domain, 'owner');
  if (!enriched?.email) {
    return {
      ...base,
      path: 'provider_chain',
      reason: 'no_email_found',
      verificationSource: enriched?.source?.join?.('+') || null,
      tiered: { errors: tieredOutcome.errors },
    };
  }

  const verification = await resolveEmailVerification(enriched.email, enriched);
  const persistResult = await persistProviderChainEmail(
    options.db,
    row,
    enriched,
    verification,
    options.dryRun
  );

  if (persistResult.persisted || persistResult.wouldPersist) {
    const finalRow = options.dryRun
      ? {
        email: enriched.email,
        email_verified: verification.emailVerified,
        email_status: verification.emailStatus,
        do_not_contact: verification.doNotContact,
      }
      : await loadProspectRow(options.db, row.client_id, row.prospect_id);

    return {
      ...base,
      verified: isProjectableCrmProspect(crmProjectionRow(finalRow)),
      persisted: persistResult.persisted === true,
      path: 'provider_chain',
      email: String(finalRow.email || enriched.email).trim(),
      emailStatus: finalRow.email_status || verification.emailStatus,
      emailVerificationMethod: finalRow.email_verification_method || verification.emailVerificationMethod,
      verificationSource: (enriched.source || ['provider_chain']).join('+'),
      reason: persistResult.persisted ? 'provider_chain_persisted' : 'provider_chain_dry_run',
      dnc: finalRow.do_not_contact === true,
    };
  }

  return {
    ...base,
    path: 'provider_chain',
    email: enriched.email,
    emailStatus: verification.emailStatus,
    emailVerificationMethod: verification.emailVerificationMethod,
    verificationSource: (enriched.source || []).join('+'),
    reason: persistResult.reason || 'failed_verification_gates',
    tiered: { errors: tieredOutcome.errors },
  };
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  EXCLUDED_COMPANY_RE,
  isExcludedCompany,
  crmProjectionRow,
  loadProspectRow,
  loadMissionBoundProspects,
  persistProviderChainEmail,
  enrichProspectRow,
};

'use strict';

/**
 * Shared enrichment helpers for Anchor mission-bound contacts.
 * Never sends mail. Never revises CAPACITY.
 */

const { getAcquisitionMissionRuntime } = require('../../services/acquisitionMissionRuntime');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
  listMissionBoundCrmLookupKeys,
  listMissionBoundProspectIds,
} = require('../../packages/max/workspace/EmmettMissionCandidates');
const {
  aliasCrmMapToIdentities,
  identityKeysFrom,
} = require('../../packages/max/workspace/CanonicalOutboundIdentity');
const {
  isProjectableCrmProspect,
  loadCrmProspectsForMissionBoundCompanies,
  loadCrmProspectsByIds,
} = require('../../packages/max/workspace/MissionBoundCrmResolver');
const {
  admitMissionBoundCandidates,
} = require('../../packages/max/workspace/MissionBoundCrmAdmission');
const {
  configureScoringContext,
  runEnrichmentChain,
  resolveEmailVerification,
  normalizeDomain,
} = require('../../leadgen');
const tiered = require('../../tieredEnrichmentAgent');
const { resolveEnrichmentDomain } = require('../../utils/websiteEnrichmentCrawl');
const {
  resolveEmailProvenanceSource,
  stampEmailProvenance,
} = require('../../utils/canonicalEmailEligibility');
const { persistableEmailSource, remediateTaintedCrmEmail } = require('../../utils/crmEmailProvenance');

const TENANT_ID = '10';
const CLIENT_ID = 10;
const DEFAULT_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const EXCLUDED_COMPANY_RE = /deliverability\s*test/i;

function isExcludedCompany(name) {
  return EXCLUDED_COMPANY_RE.test(String(name || '').trim());
}

function crmProjectionRow(row = {}, extras = {}) {
  return {
    email: row.email,
    email_verified: row.email_verified,
    email_status: row.email_status,
    do_not_contact: row.do_not_contact,
    enrichment_provenance: extras.enrichment_provenance || row.enrichment_provenance,
    email_provenance_source: extras.email_provenance_source || row.email_provenance_source || null,
    verificationSource: extras.verificationSource || row.verificationSource || null,
  };
}

async function loadProspectRow(db, clientId, prospectId) {
  const { loadBestCrmProspectForMissionBoundKey } = require('../../packages/max/workspace/MissionBoundCrmResolver');
  const byId = await loadCrmProspectsByIds({
    pool: db,
    clientId,
    prospectIds: [prospectId],
  });
  if (byId.size) return byId.values().next().value;
  return loadBestCrmProspectForMissionBoundKey({
    pool: db,
    clientId,
    missionBoundKey: prospectId,
  });
}

async function loadMissionBoundProspects(db, missionId, opts = {}) {
  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    throw Object.assign(new Error(`Mission ${missionId} not found.`), { code: 'mission_not_found' });
  }
  const snapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const contributions = snapshot.contributions || [];

  const admission = await admitMissionBoundCandidates(db, mission, contributions, {
    clientId: CLIENT_ID,
    missionId,
    dryRun: Boolean(opts.dryRun),
  });

  const candidates = buildMissionBoundCandidates(mission, contributions);
  const companyIds = listMissionBoundCompanyIds(mission, contributions);
  const lookupKeys = listMissionBoundCrmLookupKeys(mission, contributions);
  const crmProspectIds = listMissionBoundProspectIds(mission, contributions);
  const maps = [];
  if (lookupKeys.length) {
    maps.push(await loadCrmProspectsForMissionBoundCompanies({
      pool: db,
      clientId: CLIENT_ID,
      companyIds: lookupKeys,
    }));
  }
  if (crmProspectIds.length) {
    maps.push(await loadCrmProspectsByIds({
      pool: db,
      clientId: CLIENT_ID,
      prospectIds: crmProspectIds,
    }));
  }
  const crmByIdentity = maps.length
    ? aliasCrmMapToIdentities(maps, candidates)
    : new Map();
  const rowsByCompanyId = new Map();
  for (const candidate of candidates) {
    let row = null;
    for (const key of identityKeysFrom(candidate)) {
      row = crmByIdentity.get(String(key));
      if (row) break;
    }
    if (row) rowsByCompanyId.set(String(candidate.id), row);
  }
  const rows = companyIds
    .map((companyId) => rowsByCompanyId.get(String(companyId)))
    .filter(Boolean);
  return {
    mission,
    snapshot,
    companyIds,
    candidates,
    admission,
    // Legacy report field: mission-bound keys (company/candidate IDs), not CRM prospects.id.
    prospectIds: companyIds,
    rowsByCompanyId,
    rows,
  };
}

async function persistProviderChainEmail(db, row, enriched, verification, dryRun) {
  const providerSource = persistableEmailSource((enriched.source || ['provider_chain']).join('+'))
    || 'provider_chain';
  const candidate = {
    email: enriched.email,
    email_verified: verification.emailVerified === true,
    email_status: verification.emailStatus,
    do_not_contact: verification.doNotContact === true,
    email_provenance_source: providerSource,
    enrichment_provenance: stampEmailProvenance(row.enrichment_provenance, providerSource, {
      verifier: verification.emailVerificationMethod || 'bouncer',
      status: verification.emailStatus,
      resolved_at: new Date().toISOString(),
    }),
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
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $9::jsonb,
            updated_at = NOW()
      WHERE id = $10
        AND client_id = $11`,
    [
      candidate.email,
      firstName,
      lastName,
      true,
      verification.emailVerificationMethod,
      verification.emailStatus,
      JSON.stringify(verification.verifierResponse || null),
      verification.doNotContact === true,
      JSON.stringify(candidate.enrichment_provenance),
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
        email_provenance_source: providerSource,
      }),
      'success',
      row.client_id,
    ]
  );

  return { persisted: true, email: candidate.email };
}

function reportProvenanceFields(row = {}, extras = {}) {
  const enrichmentProvenance = extras.enrichment_provenance || row.enrichment_provenance || null;
  const verificationSource = extras.verificationSource
    || resolveEmailProvenanceSource({
      ...row,
      enrichment_provenance: enrichmentProvenance,
      verificationSource: extras.verificationSource,
    });
  return {
    enrichment_provenance: enrichmentProvenance,
    email_provenance_source: extras.email_provenance_source
      || row.email_provenance_source
      || verificationSource
      || null,
    verificationSource: verificationSource || null,
  };
}

async function enrichProspectRow(row, options = {}) {
  const company = row.company_name || row.company || null;
  const processProspect = options.processProspect || ((prospectRow, processOptions) => (
    tiered._test.processProspect(prospectRow, processOptions)
  ));
  const runProviders = options.runEnrichmentChain || runEnrichmentChain;
  const verifyEmailFn = options.resolveEmailVerification || resolveEmailVerification;
  const configureContext = options.configureScoringContext || configureScoringContext;
  let working = { ...row };
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
    verificationSource: resolveEmailProvenanceSource(row),
    enrichment_provenance: row.enrichment_provenance || null,
    dnc: row.do_not_contact === true,
    reason: null,
    remediation: null,
  };

  if (isExcludedCompany(company)) {
    return {
      ...base,
      excluded: true,
      reason: 'deliverability_test_excluded',
    };
  }

  const remediation = await remediateTaintedCrmEmail(options.db, working, { dryRun: options.dryRun });
  if (remediation.plan?.action && remediation.plan.action !== 'none') {
    base.remediation = {
      action: remediation.plan.action,
      reason: remediation.plan.reason,
      applied: remediation.applied === true,
      dryRun: remediation.dryRun === true,
    };
    if (remediation.applied) {
      working = {
        ...working,
        email: remediation.email === undefined ? working.email : remediation.email,
        email_verified: remediation.plan.action === 'invalidate_contaminated' ? false : working.email_verified,
        email_status: remediation.plan.action === 'invalidate_contaminated' ? 'quarantined' : working.email_status,
        enrichment_provenance: remediation.provenance || working.enrichment_provenance,
      };
    } else if (remediation.plan.action === 'invalidate_contaminated' && options.dryRun) {
      working = {
        ...working,
        email: null,
        email_verified: false,
        email_status: 'quarantined',
        enrichment_provenance: stampEmailProvenance(working.enrichment_provenance, remediation.plan.provenance, {
          invalidated: true,
          invalidation_reason: remediation.plan.reason,
          quarantined_email: remediation.plan.email,
        }),
      };
    } else if (remediation.plan.action === 'preserve_untrusted_provenance') {
      working = {
        ...working,
        enrichment_provenance: stampEmailProvenance(working.enrichment_provenance, remediation.plan.provenance, {
          outbound_eligible: false,
          preservation_reason: remediation.plan.reason,
        }),
      };
    }
  }

  const originalSource = resolveEmailProvenanceSource(working);
  if (isProjectableCrmProspect(crmProjectionRow(working))) {
    return {
      ...base,
      ...reportProvenanceFields(working, { verificationSource: originalSource || 'existing_crm' }),
      verified: true,
      persisted: true,
      path: 'existing_crm',
      email: String(working.email).trim(),
      emailStatus: working.email_status,
      emailVerificationMethod: working.email_verification_method,
      reason: 'already_projectable',
      dnc: working.do_not_contact === true,
    };
  }

  const tieredOutcome = await processProspect(working, {
    dryRun: options.dryRun,
    fetchDelayMs: options.fetchDelayMs,
  });

  const reloaded = options.dryRun
    ? working
    : await loadProspectRow(options.db, working.client_id, working.prospect_id) || working;

  const tieredVerificationSource = persistableEmailSource(tieredOutcome.selectedEmail?.source)
    || resolveEmailProvenanceSource(reloaded);
  if (reloaded && isProjectableCrmProspect(crmProjectionRow(reloaded, {
    verificationSource: tieredVerificationSource,
    enrichment_provenance: reloaded.enrichment_provenance,
  }))) {
    return {
      ...base,
      ...reportProvenanceFields(reloaded, { verificationSource: tieredVerificationSource }),
      verified: true,
      persisted: !options.dryRun,
      path: 'tiered_enrichment',
      email: String(reloaded.email).trim(),
      emailStatus: reloaded.email_status,
      emailVerificationMethod: reloaded.email_verification_method,
      reason: tieredOutcome.resolved ? 'tiered_resolved' : 'tiered_email_persisted',
      dnc: reloaded.do_not_contact === true,
      tiered: {
        resolved: tieredOutcome.resolved,
        resolvedTier: tieredOutcome.resolvedTier,
        errors: tieredOutcome.errors,
      },
    };
  }

  const domain = resolveEnrichmentDomain(working);
  if (!domain) {
    return {
      ...base,
      ...reportProvenanceFields(working, { verificationSource: originalSource }),
      path: 'provider_chain',
      email: working.email || null,
      emailStatus: working.email_status || null,
      reason: 'no_domain',
      tiered: { errors: tieredOutcome.errors },
    };
  }

  await configureContext({ client_id: working.client_id || CLIENT_ID });
  const enriched = await runProviders(domain, 'owner');
  if (!enriched?.email) {
    return {
      ...base,
      ...reportProvenanceFields(working, {
        verificationSource: persistableEmailSource(enriched?.source?.join?.('+')) || originalSource,
      }),
      path: 'provider_chain',
      email: working.email || null,
      emailStatus: working.email_status || null,
      reason: 'no_email_found',
      tiered: { errors: tieredOutcome.errors },
    };
  }

  const verification = await verifyEmailFn(enriched.email, enriched);
  const persistResult = await persistProviderChainEmail(
    options.db,
    working,
    enriched,
    verification,
    options.dryRun
  );

  if (persistResult.persisted || persistResult.wouldPersist) {
    const providerSource = persistableEmailSource((enriched.source || ['provider_chain']).join('+'))
      || 'provider_chain';
    const finalRow = options.dryRun
      ? {
        email: enriched.email,
        email_verified: verification.emailVerified,
        email_status: verification.emailStatus,
        do_not_contact: verification.doNotContact,
        enrichment_provenance: stampEmailProvenance(working.enrichment_provenance, providerSource, {
          verifier: verification.emailVerificationMethod || 'bouncer',
          status: verification.emailStatus,
        }),
        email_provenance_source: providerSource,
      }
      : await loadProspectRow(options.db, working.client_id, working.prospect_id);

    return {
      ...base,
      ...reportProvenanceFields(finalRow, { verificationSource: providerSource }),
      verified: isProjectableCrmProspect(crmProjectionRow(finalRow, {
        verificationSource: providerSource,
      })),
      persisted: persistResult.persisted === true,
      path: 'provider_chain',
      email: String(finalRow.email || enriched.email).trim(),
      emailStatus: finalRow.email_status || verification.emailStatus,
      emailVerificationMethod: finalRow.email_verification_method || verification.emailVerificationMethod,
      reason: persistResult.persisted ? 'provider_chain_persisted' : 'provider_chain_dry_run',
      dnc: finalRow.do_not_contact === true,
    };
  }

  return {
    ...base,
    ...reportProvenanceFields(working, {
      verificationSource: persistableEmailSource((enriched.source || []).join('+')) || originalSource,
    }),
    path: 'provider_chain',
    email: enriched.email,
    emailStatus: verification.emailStatus,
    emailVerificationMethod: verification.emailVerificationMethod,
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
  admitMissionBoundCandidates,
  persistProviderChainEmail,
  enrichProspectRow,
  reportProvenanceFields,
};

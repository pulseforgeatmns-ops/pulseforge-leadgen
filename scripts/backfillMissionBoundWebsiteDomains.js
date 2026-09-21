#!/usr/bin/env node
'use strict';

/**
 * Idempotent repair for mission-bound CRM admissions missing website/domain.
 *
 * Dry-run by default. Production apply requires --confirm-production.
 *
 * Usage:
 *   node scripts/backfillMissionBoundWebsiteDomains.js \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 *
 *   node scripts/backfillMissionBoundWebsiteDomains.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 */

require('dotenv').config();

const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
} = require('../packages/max/workspace/EmmettMissionCandidates');
const {
  resolvedCandidateWebsite,
  admissionProvenance,
  ADMISSION_SOURCE,
} = require('../packages/max/workspace/MissionBoundCrmAdmission');
const { normalizeDomain } = require('../packages/max/workspace/CanonicalOutboundIdentity');
const { DEFAULT_MISSION_ID } = require('./lib/anchorMissionBoundEnrichment');

const TENANT_ID = '10';
const CLIENT_ID = 10;

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const dryRun = !confirmProduction || argv.includes('--dry-run');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  return { confirmProduction, dryRun, missionId };
}

async function loadAdmittedRows(db, missionId, candidates) {
  const placeIds = candidates.map((row) => row.placeId).filter(Boolean);
  if (!placeIds.length) return new Map();

  const { rows } = await db.query(
    `SELECT c.id AS company_id,
            c.name AS company_name,
            c.domain,
            c.website,
            c.google_place_id,
            c.enrichment_provenance,
            p.id AS prospect_id,
            p.website_url,
            p.has_website,
            p.source,
            p.discovery_method
       FROM companies c
       JOIN prospects p ON p.company_id = c.id
      WHERE c.client_id = $1
        AND p.client_id = $1
        AND COALESCE(p.is_synthetic, false) = false
        AND p.source = $2
        AND p.discovery_method = 'mission_bound'
        AND c.google_place_id = ANY($3::text[])`,
    [CLIENT_ID, ADMISSION_SOURCE, placeIds]
  );

  const byPlaceId = new Map();
  for (const row of rows) {
    byPlaceId.set(String(row.google_place_id), row);
  }
  return byPlaceId;
}

function buildRepairPlan(candidate, crmRow) {
  const upstream = resolvedCandidateWebsite(candidate);
  if (!upstream.domain && !upstream.website) {
    return {
      action: 'skip_no_upstream_website',
      candidateId: candidate.candidateId,
      placeId: candidate.placeId,
      company: candidate.company,
    };
  }

  const updates = {
    companyDomain: null,
    companyWebsite: null,
    prospectWebsiteUrl: null,
    prospectHasWebsite: null,
  };
  const reasons = [];

  if (!crmRow.domain && upstream.domain) {
    updates.companyDomain = upstream.domain;
    reasons.push('backfill_company_domain');
  } else if (crmRow.domain && upstream.domain && normalizeDomain(crmRow.domain) !== upstream.domain) {
    return {
      action: 'skip_existing_domain',
      candidateId: candidate.candidateId,
      placeId: candidate.placeId,
      company: candidate.company,
      existingDomain: crmRow.domain,
      upstreamDomain: upstream.domain,
    };
  }

  if (!crmRow.website && upstream.website) {
    updates.companyWebsite = upstream.website;
    reasons.push('backfill_company_website');
  }

  if (!crmRow.website_url && upstream.website) {
    updates.prospectWebsiteUrl = upstream.website;
    updates.prospectHasWebsite = upstream.hasWebsite;
    reasons.push('backfill_prospect_website_url');
  }

  if (!reasons.length) {
    return {
      action: 'noop_already_canonical',
      candidateId: candidate.candidateId,
      placeId: candidate.placeId,
      company: candidate.company,
      domain: crmRow.domain,
      website: crmRow.website,
      website_url: crmRow.website_url,
    };
  }

  return {
    action: 'repair',
    candidateId: candidate.candidateId,
    placeId: candidate.placeId,
    company: candidate.company,
    companyId: crmRow.company_id,
    prospectId: crmRow.prospect_id,
    updates,
    reasons,
    upstream,
    provenance: admissionProvenance(candidate, candidate.missionId),
  };
}

async function applyRepair(db, plan, dryRun) {
  if (plan.action !== 'repair') return plan;
  if (dryRun) return { ...plan, applied: false, dryRun: true };

  const provenance = plan.provenance;
  await db.query(
    `UPDATE companies
        SET domain = COALESCE(domain, $1),
            website = COALESCE(website, $2),
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $3::jsonb,
            updated_at = NOW()
      WHERE id = $4
        AND client_id = $5`,
    [
      plan.updates.companyDomain,
      plan.updates.companyWebsite,
      JSON.stringify(provenance),
      plan.companyId,
      CLIENT_ID,
    ]
  );

  await db.query(
    `UPDATE prospects
        SET website_url = COALESCE(NULLIF(TRIM(website_url), ''), $1),
            has_website = CASE
              WHEN COALESCE(has_website, false) = true THEN has_website
              ELSE $2
            END,
            enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $3::jsonb,
            updated_at = NOW()
      WHERE id = $4
        AND client_id = $5`,
    [
      plan.updates.prospectWebsiteUrl,
      plan.updates.prospectHasWebsite,
      JSON.stringify(provenance),
      plan.prospectId,
      CLIENT_ID,
    ]
  );

  return { ...plan, applied: true, dryRun: false };
}

async function run(opts = {}) {
  const missionId = opts.missionId || DEFAULT_MISSION_ID;
  const dryRun = opts.dryRun !== false;
  const db = opts.db || pool;

  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    throw Object.assign(new Error(`Mission ${missionId} not found.`), { code: 'mission_not_found' });
  }
  const snapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  const contributions = snapshot.contributions || [];
  const candidates = buildMissionBoundCandidates(mission, contributions)
    .filter((row) => listMissionBoundCompanyIds(mission, contributions).includes(String(row.id)))
    .map((row) => ({ ...row, missionId }));

  const admittedByPlace = await loadAdmittedRows(db, missionId, candidates);
  const plans = [];
  for (const candidate of candidates) {
    const crmRow = admittedByPlace.get(String(candidate.placeId));
    if (!crmRow) {
      plans.push({
        action: 'skip_not_admitted',
        candidateId: candidate.candidateId,
        placeId: candidate.placeId,
        company: candidate.company,
      });
      continue;
    }
    const plan = buildRepairPlan(candidate, crmRow);
    plans.push(await applyRepair(db, plan, dryRun));
  }

  return {
    missionId,
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    dryRun,
    candidateCount: candidates.length,
    admittedCount: [...admittedByPlace.keys()].length,
    repairCount: plans.filter((row) => row.action === 'repair').length,
    appliedCount: plans.filter((row) => row.applied === true).length,
    withUpstreamDomain: candidates.filter((row) => resolvedCandidateWebsite(row).domain).length,
    plans,
  };
}

async function main() {
  const args = parseArgs();
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing DATABASE_URL'), { code: 'runtime_env_missing' });
  }
  const report = await run(args);
  console.log(JSON.stringify(report, null, 2));
}

if (require.main === module) {
  main().catch((err) => {
    console.error(err.message || err);
    process.exitCode = 1;
  });
}

module.exports = {
  parseArgs,
  buildRepairPlan,
  run,
};

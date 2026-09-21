#!/usr/bin/env node
'use strict';

/**
 * Canonical website discovery for Anchor mission-bound candidates.
 * Runs before contact enrichment. No mail side effects.
 *
 * Dry-run by default. Production persistence requires --confirm-production.
 *
 * Usage:
 *   node scripts/discoverAnchorMissionBoundWebsites.js \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 *
 *   node scripts/discoverAnchorMissionBoundWebsites.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
 */

require('dotenv').config();

const pool = require('../db');
const { getAcquisitionMissionRuntime } = require('../services/acquisitionMissionRuntime');
const {
  buildMissionBoundCandidates,
  listMissionBoundCompanyIds,
} = require('../packages/max/workspace/EmmettMissionCandidates');
const { ADMISSION_SOURCE } = require('../packages/max/workspace/MissionBoundCrmAdmission');
const {
  discoverMissionBoundWebsite,
  buildDiscoveryPlan,
  applyDiscoveryPlan,
} = require('../packages/max/workspace/MissionBoundWebsiteDiscovery');
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

async function loadAdmittedRows(db, candidates) {
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

function summarizePlans(plans) {
  const missingWebsiteCount = plans.filter(
    (row) => !row.currentDomain && !row.currentWebsite
  ).length;
  return {
    candidateCount: plans.length,
    missingWebsiteCount,
    resolvedWebsiteCount: plans.filter(
      (row) => row.action === 'persist' || (row.resolvedDomain && row.action === 'manual_review')
    ).length,
    persistedWebsiteCount: plans.filter((row) => row.persisted === true).length,
    manualReviewCount: plans.filter((row) => row.action === 'manual_review').length,
  };
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

  const admittedByPlace = await loadAdmittedRows(db, candidates);
  const plans = [];

  for (const candidate of candidates) {
    const crmRow = admittedByPlace.get(String(candidate.placeId));
    if (!crmRow) {
      plans.push({
        company: candidate.company,
        candidateId: candidate.candidateId,
        placeId: candidate.placeId,
        action: 'skip_not_admitted',
        persisted: false,
        reason: 'not_admitted',
      });
      continue;
    }

    const resolution = await discoverMissionBoundWebsite(candidate, {
      crmRow,
      missionId,
      tenantId: TENANT_ID,
      apiKey: opts.apiKey,
      fetchImpl: opts.fetchImpl,
      placesDeps: opts.placesDeps,
    });
    const plan = buildDiscoveryPlan(candidate, crmRow, resolution, missionId);
    const applied = await applyDiscoveryPlan(db, plan, dryRun, CLIENT_ID);
    plans.push({
      ...applied,
      reason: applied.reason || resolution.reason || null,
    });
  }

  return {
    missionId,
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    dryRun,
    ...summarizePlans(plans),
    companies: plans,
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
  run,
  loadAdmittedRows,
  summarizePlans,
};

#!/usr/bin/env node
'use strict';

/**
 * Trace upstream website/domain fields for mission-bound candidates.
 *
 * Usage:
 *   node scripts/traceMissionBoundWebsiteIntel.js \
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
  collectWebsiteFieldTraces,
  resolveMissionBoundWebsiteIntel,
} = require('../packages/max/workspace/MissionBoundWebsiteIntel');
const { DEFAULT_MISSION_ID } = require('./lib/anchorMissionBoundEnrichment');

const TENANT_ID = '10';
const CLIENT_ID = 10;

function parseArgs(argv = process.argv.slice(2)) {
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  const companyFilter = argv.includes('--company')
    ? argv[argv.indexOf('--company') + 1]
    : null;
  return { missionId, companyFilter };
}

function unwrapPayload(payload = {}) {
  if (payload?.payload && typeof payload.payload === 'object') {
    return payload.payload.payload || payload.payload;
  }
  return payload.payload || payload;
}

async function loadMissionContext(db, missionId) {
  const runtime = getAcquisitionMissionRuntime({ production: true, persist: true, pool: db });
  await runtime.hydrate(TENANT_ID, { pool: db, production: true });
  const engine = runtime.engine();
  const mission = engine.get(missionId, TENANT_ID);
  if (!mission) {
    throw Object.assign(new Error(`Mission ${missionId} not found.`), { code: 'mission_not_found' });
  }
  const snapshot = engine.inspect(missionId, { tenantId: TENANT_ID });
  return { mission, snapshot, contributions: snapshot.contributions || [] };
}

function buildTraceRows(mission, contributions, companyFilter = null) {
  const candidates = buildMissionBoundCandidates(mission, contributions);
  const prioritizedIds = new Set(listMissionBoundCompanyIds(mission, contributions).map(String));
  const scout = contributions.find((row) => row.specialist === 'scout' && row.kind === 'discovery');
  const max = contributions.find((row) => row.specialist === 'max' && row.kind === 'prioritization');
  const scoutPayload = unwrapPayload(scout?.payload || {});
  const maxPayload = unwrapPayload(max?.payload || {});

  const opportunities = scoutPayload.opportunities || [];
  const prospects = scoutPayload.prospects || [];
  const rankedTargets = maxPayload.rankedTargets || maxPayload.priorities || [];

  return candidates
    .filter((candidate) => prioritizedIds.has(String(candidate.id)))
    .filter((candidate) => !companyFilter || String(candidate.company || '').toLowerCase().includes(String(companyFilter).toLowerCase()))
    .map((candidate) => {
      const target = rankedTargets.find((row) =>
        String(row.companyId || row.id || row.placeId || '') === String(candidate.id)
        || String(row.name || '') === String(candidate.company || '')) || {};
      const opp = opportunities.find((row) =>
        String(row.companyId || row.id || row.placeId || '') === String(candidate.id)
        || String(row.name || '') === String(candidate.company || '')) || {};
      const prospect = prospects.find((row) =>
        String(row.companyId || row.id || row.placeId || '') === String(candidate.id)
        || String(row.company || row.name || '') === String(candidate.company || '')) || null;

      const traces = collectWebsiteFieldTraces({ target, opp, prospect });
      const resolved = resolveMissionBoundWebsiteIntel({ target, opp, prospect });

      return {
        company: candidate.company,
        candidateId: candidate.candidateId,
        placeId: candidate.placeId,
        resolvedDomain: resolved.domain,
        resolvedWebsite: resolved.website,
        resolvedSource: resolved.source,
        resolvedFieldPath: resolved.fieldPath,
        candidateBuilderDomain: candidate.domain || null,
        candidateBuilderWebsite: candidate.website || candidate.website_url || null,
        upstreamTraces: traces,
        scoutOpportunityFields: {
          website: opp.website || null,
          website_url: opp.website_url || null,
          url: opp.url || null,
          sourceUrl: opp.sourceUrl || opp.source_url || null,
          domain: opp.domain || null,
          companyDomain: opp.companyDomain || opp.company_domain || null,
          normalizedDomain: opp.normalizedDomain || opp.normalized_domain || null,
        },
        maxTargetFields: {
          website: target.website || null,
          website_url: target.website_url || null,
          url: target.url || null,
          sourceUrl: target.sourceUrl || target.source_url || null,
          domain: target.domain || null,
          companyDomain: target.companyDomain || target.company_domain || null,
          normalizedDomain: target.normalizedDomain || target.normalized_domain || null,
        },
      };
    });
}

async function run(opts = {}) {
  const missionId = opts.missionId || DEFAULT_MISSION_ID;
  const db = opts.db || pool;
  const { mission, contributions } = await loadMissionContext(db, missionId);
  const rows = buildTraceRows(mission, contributions, opts.companyFilter || null);
  return {
    missionId,
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    candidateCount: rows.length,
    withDomain: rows.filter((row) => row.resolvedDomain).length,
    rows,
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
  buildTraceRows,
  run,
};

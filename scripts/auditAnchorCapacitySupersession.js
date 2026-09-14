#!/usr/bin/env node
'use strict';

/**
 * Read-only audit (+ optional repair) for Emmett CAPACITY supersession on a mission.
 *
 * Usage:
 *   node scripts/auditAnchorCapacitySupersession.js --confirm-production \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750
 *
 * Repair (marks every non-active CAPACITY superseded durably):
 *   node scripts/auditAnchorCapacitySupersession.js --confirm-production --repair \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750
 */

require('dotenv').config();

const amo = require('../packages/acquisition-mission');
const { SPECIALISTS, CONTRIBUTION_KINDS, isSupersededContribution, markContributionSuperseded } = amo;
const pool = require('../db');
const {
  loadMissionSnapshot,
  persistStageCommit,
} = require('../services/acquisitionMissionPersistence');
const {
  selectActiveCapacityContribution,
  loadCapacityRowsForMission,
  unwrapMissionPayload,
} = require('./lib/activeCapacitySelection');

const TENANT_ID = '10';
const DEFAULT_MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const repair = argv.includes('--repair');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  return { confirmProduction, repair, help, missionId };
}

function summarizeCapacityRow(row) {
  const body = row.payload || {};
  return {
    id: row.id || row.capacity_id,
    at: row.at,
    topLevelSuperseded: body.superseded === true,
    nestedSuperseded: body.payload?.superseded === true,
    canonicalSuperseded: isSupersededContribution(row),
    supersededBy: body.supersededBy || body.payload?.supersededBy || null,
  };
}

async function run(options = {}) {
  if (options.help) {
    console.log(`Audit Anchor CAPACITY supersession (tenant ${TENANT_ID})`);
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing required runtime env: DATABASE_URL');
    err.code = 'runtime_env_missing';
    throw err;
  }

  const missionId = options.missionId;
  const durable = await loadMissionSnapshot(missionId, TENANT_ID, pool);
  if (!durable?.mission) {
    const err = new Error(`Mission ${missionId} not found for tenant ${TENANT_ID}.`);
    err.code = 'mission_not_found';
    throw err;
  }

  const capacityRows = await loadCapacityRowsForMission(pool, TENANT_ID, missionId);
  const missionBody = unwrapMissionPayload(durable.mission);
  const active = selectActiveCapacityContribution(missionBody, capacityRows);
  const contributions = (durable.contributions || []).filter(
    (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
  );

  const activeRows = contributions.filter((row) => !isSupersededContribution(row));
  const staleRows = contributions.filter((row) => isSupersededContribution(row));

  const report = {
    tenantId: TENANT_ID,
    missionId,
    stage: durable.mission.stage,
    revisionPointer: missionBody.revisionState?.emmettContributionId || null,
    capacityRows: contributions.map(summarizeCapacityRow),
    activeCapacityId: active?.capacity_id || active?.id || null,
    activeNonSupersededCount: activeRows.length,
    staleCount: staleRows.length,
    requiresRepair: activeRows.length !== 1,
    repairPerformed: false,
    repairedRowIds: [],
  };

  if (options.repair && report.requiresRepair && active?.capacity_id) {
    const activeId = String(active.capacity_id);
    const repairedRowIds = [];
    const repairedContributions = contributions.map((row) => {
      if (String(row.id) === activeId || isSupersededContribution(row)) return row;
      repairedRowIds.push(row.id);
      return markContributionSuperseded(row, activeId);
    });
    if (repairedRowIds.length) {
      await persistStageCommit({
        mission: durable.mission,
        contributions: [
          ...(durable.contributions || []).filter(
            (row) => !(row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY)
          ),
          ...repairedContributions,
        ],
        events: [],
        observations: [],
        outcomes: [],
      }, pool, { skipGlobalLock: true });
      report.repairPerformed = true;
      report.repairedRowIds = repairedRowIds;
    }
    const reloaded = await loadMissionSnapshot(missionId, TENANT_ID, pool);
    const reloadedCapacities = (reloaded.contributions || []).filter(
      (row) => row.specialist === SPECIALISTS.EMMETT && row.kind === CONTRIBUTION_KINDS.CAPACITY
    );
    report.activeNonSupersededCount = reloadedCapacities.filter(
      (row) => !isSupersededContribution(row)
    ).length;
    report.requiresRepair = report.activeNonSupersededCount !== 1;
    report.capacityRows = reloadedCapacities.map(summarizeCapacityRow);
  }

  return report;
}

module.exports = { run, summarizeCapacityRow };

if (require.main === module) {
  run(parseArgs())
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.requiresRepair ? 2 : 0;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

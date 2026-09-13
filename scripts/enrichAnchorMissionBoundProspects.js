#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — enrich verified emails for existing mission-bound prospects only.
 *
 * Uses canonical tiered enrichment (website + Bouncer) then provider chain
 * (Prospeo → Hunter → scrape + verifyEmail). Never expands mission universe.
 * Never sends mail.
 *
 * Railway:
 *   node scripts/enrichAnchorMissionBoundProspects.js --confirm-production \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750
 *
 * After at least one verified email is persisted, optionally regenerates CAPACITY
 * (REVISE_PREPARED_OUTREACH) and runs auditAnchorCapacitySendability.
 */

require('dotenv').config();

const pool = require('../db');
const { ensureTieredEnrichmentSchema } = require('../utils/tieredEnrichmentSchema');
const { ensureEmailVerificationColumns } = require('../utils/emailVerificationSchema');
const regenerate = require('./regenerateAnchorCapacityRevision');
const audit = require('./auditAnchorCapacitySendability');
const {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  EXCLUDED_COMPANY_RE,
  isExcludedCompany,
  loadMissionBoundProspects,
  enrichProspectRow,
} = require('./lib/anchorMissionBoundEnrichment');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const dryRun = argv.includes('--dry-run');
  const skipRevise = argv.includes('--skip-revise');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  const unknown = argv.filter(
    (arg, i) =>
      arg !== '--confirm-production'
      && arg !== '--dry-run'
      && arg !== '--skip-revise'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--mission-id'
      && (missionIdx < 0 || i !== missionIdx + 1)
  );
  if (!missionId || missionId.startsWith('--')) {
    throw Object.assign(new Error('--mission-id requires a mission id value.'), { code: 'mission_id_required' });
  }
  if (unknown.length) {
    throw Object.assign(new Error(`Unknown argument(s): ${unknown.join(', ')}.`), { code: 'unknown_args' });
  }
  return { confirmProduction, dryRun, skipRevise, help, missionId };
}

function printUsage() {
  console.log(`Anchor mission-bound prospect enrichment (tenant ${TENANT_ID})

Usage:
  node scripts/enrichAnchorMissionBoundProspects.js --confirm-production [--mission-id <id>] [--dry-run] [--skip-revise]

Safety:
  Refuses without --confirm-production.
  Mission-bound prospects only — never adds companies to the mission.
  Never infers/synthesizes emails.
  Excludes "Deliverability Test" from outbound enrichment.
  Persists only emails passing canonical outreach safety gates.
  Never sends mail.
`);
}

function assertRuntimeEnv() {
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing required runtime env: DATABASE_URL'), { code: 'runtime_env_missing' });
  }
  if (
    process.env.ALLOW_FIXTURE_FALLBACK === 'true'
    || process.env.allowFixtureFallback === 'true'
  ) {
    throw Object.assign(new Error('Refusing to run with ALLOW_FIXTURE_FALLBACK enabled.'), { code: 'fixture_fallback_env' });
  }
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), { code: 'confirm_production_required' });
  }
  assertRuntimeEnv();

  const db = options.pool || pool;
  const missionId = options.missionId || DEFAULT_MISSION_ID;
  const dryRun = Boolean(options.dryRun);
  const skipRevise = Boolean(options.skipRevise);

  await ensureEmailVerificationColumns();
  await ensureTieredEnrichmentSchema();

  const { mission, rows, prospectIds } = await loadMissionBoundProspects(db, missionId);
  const results = [];
  for (const row of rows) {
    results.push(await enrichProspectRow(row, {
      db,
      dryRun,
      fetchDelayMs: options.fetchDelayMs,
    }));
  }

  const verifiedCount = results.filter((row) => row.verified && !row.excluded).length;
  const persistedCount = results.filter((row) => row.persisted && !row.excluded).length;

  let revision = null;
  let auditReport = null;

  if (verifiedCount > 0 && persistedCount > 0 && !dryRun && !skipRevise) {
    revision = await regenerate.run({
      confirmProduction: true,
      missionId,
      pool: db,
    });
    auditReport = await audit.run({
      confirmProduction: true,
      missionId,
      pool: db,
    });
  }

  const sendableItems = auditReport?.queueItems?.filter((row) => row.sendableByScript) || [];
  const oneItemSendSafe = Boolean(
    auditReport
    && auditReport.spec212?.valid === true
    && auditReport.sendableCount >= 1
    && sendableItems.some((row) => row.emailOnQueueItem)
  );

  return {
    tenantId: TENANT_ID,
    missionId,
    missionBoundProspectIds: prospectIds,
    dryRun,
    enrichment: results,
    summary: {
      missionBoundCount: rows.length,
      excludedCount: results.filter((row) => row.excluded).length,
      verifiedCount,
      persistedCount,
      newlyVerified: results.filter((row) => row.persisted && row.path !== 'existing_crm').length,
    },
    revision: revision
      ? {
        newCapacityId: revision.contributions?.newCapacityId || null,
        supersededCapacityId: revision.contributions?.supersededCapacityId || null,
        spec212After: revision.spec212?.after || null,
        probeFirstBlocker: revision.probe?.firstBlocker || null,
      }
      : null,
    audit: auditReport
      ? {
        capacityContributionId: auditReport.capacityContributionId,
        queueItemCount: auditReport.queueItemCount,
        sendableCount: auditReport.sendableCount,
        spec212Valid: auditReport.spec212?.valid === true,
        firstBlocker: auditReport.firstBlocker,
        queueItems: auditReport.queueItems,
      }
      : null,
    oneItemSendSafe,
    completedAt: new Date().toISOString(),
  };
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  EXCLUDED_COMPANY_RE,
  parseArgs,
  isExcludedCompany,
  loadMissionBoundProspects,
  enrichProspectRow,
  run,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.summary?.verifiedCount > 0 ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — production enrichment runner for existing mission-bound
 * contacts on mission_ad7753b0-6def-441d-bb1a-3764656f5750.
 *
 * Uses canonical enrichment/provider paths only (tiered website+Bouncer, then
 * Prospeo → Hunter → scrape + verifyEmail). Never invents addresses.
 * Never expands the mission candidate universe. Never sends mail.
 * Never enables autosend. Never changes enabled_agents.
 * Never regenerates CAPACITY — PREPARE revision stays a separate step.
 *
 * Railway:
 *   node scripts/enrichAnchorMissionBoundContacts.js --confirm-production \
 *     --mission-id mission_ad7753b0-6def-441d-bb1a-3764656f5750
 */

require('dotenv').config();

const pool = require('../db');
const { isProjectableCrmProspect } = require('../packages/max/workspace/MissionBoundCrmResolver');
const { ensureTieredEnrichmentSchema } = require('../utils/tieredEnrichmentSchema');
const { ensureEmailVerificationColumns } = require('../utils/emailVerificationSchema');
const {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  isExcludedCompany,
  loadMissionBoundProspects,
  enrichProspectRow,
} = require('./lib/anchorMissionBoundEnrichment');

const RAILWAY_COMMAND = [
  'node scripts/enrichAnchorMissionBoundContacts.js --confirm-production \\',
  `  --mission-id ${DEFAULT_MISSION_ID}`,
].join('\n');

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const dryRun = argv.includes('--dry-run');
  const help = argv.includes('--help') || argv.includes('-h');
  const missionIdx = argv.indexOf('--mission-id');
  const missionId = missionIdx >= 0 ? argv[missionIdx + 1] : DEFAULT_MISSION_ID;
  const unknown = argv.filter(
    (arg, i) =>
      arg !== '--confirm-production'
      && arg !== '--dry-run'
      && arg !== '--help'
      && arg !== '-h'
      && arg !== '--mission-id'
      && (missionIdx < 0 || i !== missionIdx + 1)
  );
  if (!missionId || missionId.startsWith('--')) {
    throw Object.assign(new Error('--mission-id requires a mission id value.'), { code: 'mission_id_required' });
  }
  if (unknown.length) {
    throw Object.assign(
      new Error(
        `Unknown argument(s): ${unknown.join(', ')}. Usage: node scripts/enrichAnchorMissionBoundContacts.js --confirm-production [--mission-id <id>]`
      ),
      { code: 'unknown_args' }
    );
  }
  return { confirmProduction, dryRun, help, missionId };
}

function printUsage() {
  console.log(`Anchor mission-bound contact enrichment (tenant ${TENANT_ID})

Usage:
  node scripts/enrichAnchorMissionBoundContacts.js --confirm-production [--mission-id <id>] [--dry-run]

Canonical path:
  Existing mission-bound prospect IDs only → tiered enrichment → provider chain
  Persist only when email_verified=true, email_status in {valid, verified},
  do_not_contact=false, and invalidOutreachEmailReason passes.

Safety:
  Refuses without --confirm-production.
  Does not expand the candidate universe.
  Excludes "Deliverability Test" from real outbound eligibility.
  Never infers/synthesizes emails.
  Never sends mail.
  Never enables autosend or changes enabled_agents.
  Never regenerates CAPACITY (run regenerateAnchorCapacityRevision.js separately).
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

function yesNo(value) {
  return value === true ? 'yes' : 'no';
}

function displayEmail(value) {
  const email = String(value || '').trim();
  return email || '(none)';
}

function displayVerification(row = {}) {
  const status = row.emailStatus || row.email_status || '(none)';
  const source = row.verificationSource || row.emailVerificationMethod || row.email_verification_method || '(none)';
  return `${status} / ${source}`;
}

function isEligibleForCapacityProjection(row = {}) {
  if (row.excluded === true || isExcludedCompany(row.company)) return false;
  if (row.dnc === true) return false;
  return isProjectableCrmProspect({
    email: row.email,
    email_verified: row.verified === true || row.email_verified === true,
    email_status: row.emailStatus || row.email_status,
    do_not_contact: row.dnc === true || row.do_not_contact === true,
  });
}

function missingCrmResult(prospectId) {
  return {
    prospectId: String(prospectId),
    company: null,
    excluded: false,
    verified: false,
    persisted: false,
    path: null,
    email: null,
    emailStatus: null,
    emailVerificationMethod: null,
    verificationSource: null,
    dnc: null,
    reason: 'not_found_in_crm',
  };
}

function formatContactLine(row) {
  return [
    `Prospect ID: ${row.prospectId}`,
    `Company: ${row.company || '(not found in CRM)'}`,
    `Discovered email: ${displayEmail(row.email)}`,
    `Verification status/source: ${displayVerification(row)}`,
    `Persisted to CRM: ${yesNo(row.persisted === true && row.path !== 'existing_crm')}`,
    `DNC state: ${row.dnc == null ? '(unknown)' : String(row.dnc)}`,
    `Eligible for CAPACITY projection: ${yesNo(isEligibleForCapacityProjection(row))}`,
  ].join('\n');
}

function printReport(report) {
  const lines = [
    `Anchor mission-bound contact enrichment`,
    `mission: ${report.missionId}`,
    `tenant: ${report.tenantId}`,
    report.dryRun ? 'mode: dry-run (no CRM writes)' : 'mode: production persist',
    '',
  ];
  for (const row of report.contacts || []) {
    lines.push(formatContactLine(row));
    lines.push('');
  }
  lines.push(
    `Count of mission-bound prospects now eligible for CAPACITY projection: ${report.eligibleForCapacityProjection}`
  );
  lines.push('');
  lines.push('Do not regenerate CAPACITY from this script. Next step is a separate PREPARE revision:');
  lines.push('  node scripts/regenerateAnchorCapacityRevision.js --confirm-production \\');
  lines.push(`    --mission-id ${report.missionId}`);
  lines.push('');
  lines.push('Railway command for this enrichment runner:');
  lines.push(`  ${RAILWAY_COMMAND.replace(/\n/g, '\n  ')}`);
  const text = lines.join('\n');
  console.log(text);
  return text;
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

  await ensureEmailVerificationColumns();
  await ensureTieredEnrichmentSchema();

  const { rows, prospectIds } = await loadMissionBoundProspects(db, missionId);
  const byId = new Map(rows.map((row) => [String(row.prospect_id), row]));
  const contacts = [];

  for (const prospectId of prospectIds) {
    const row = byId.get(String(prospectId));
    if (!row) {
      contacts.push(missingCrmResult(prospectId));
      continue;
    }
    contacts.push(await enrichProspectRow(row, {
      db,
      dryRun,
      fetchDelayMs: options.fetchDelayMs,
    }));
  }

  const eligibleForCapacityProjection = contacts.filter(isEligibleForCapacityProjection).length;

  const report = {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    missionId,
    dryRun,
    missionBoundProspectIds: prospectIds,
    contacts,
    eligibleForCapacityProjection,
    railwayCommand: RAILWAY_COMMAND,
    completedAt: new Date().toISOString(),
  };

  if (options.print !== false) {
    printReport(report);
  }
  return report;
}

module.exports = {
  TENANT_ID,
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  RAILWAY_COMMAND,
  parseArgs,
  isEligibleForCapacityProjection,
  formatContactLine,
  printReport,
  missingCrmResult,
  run,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      process.exitCode = report.eligibleForCapacityProjection > 0 ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        error: { code: err.code || null, message: err.message },
        railwayCommand: RAILWAY_COMMAND,
        completedAt: new Date().toISOString(),
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}

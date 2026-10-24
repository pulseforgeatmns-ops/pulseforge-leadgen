#!/usr/bin/env node
'use strict';

/**
 * Anchor tenant 10 — production enrichment runner for mission-bound contacts on
 * mission_82e8102f-249c-4f44-b88e-2de76b13898e (Anchor STR).
 *
 * Canonical path: CRM admission for prioritized Scout candidates → tiered
 * enrichment → provider chain. Never invents addresses.
 * Never expands the mission candidate universe. Never sends mail.
 * Never enables autosend. Never changes enabled_agents.
 * Never regenerates CAPACITY — PREPARE revision stays a separate step.
 *
 * Railway:
 *   node scripts/enrichAnchorMissionBoundContacts.js --confirm-production \
 *     --mission-id mission_82e8102f-249c-4f44-b88e-2de76b13898e
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
  Mission-bound CRM admission → tiered enrichment → provider chain
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
    verificationSource: row.verificationSource,
    email_provenance_source: row.email_provenance_source,
    enrichment_provenance: row.enrichment_provenance,
  });
}

function missingCrmResult(missionBoundKey) {
  return {
    prospectId: String(missionBoundKey),
    missionBoundCompanyId: String(missionBoundKey),
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
  const lines = [
    `Prospect ID: ${row.prospectId}`,
    `CRM company UUID: ${row.crmCompanyId || '(none)'}`,
    `CRM prospect UUID: ${row.crmProspectUuid || '(none)'}`,
    `Company: ${row.company || '(not found in CRM)'}`,
    `Discovered email: ${displayEmail(row.email)}`,
    `Verification status/source: ${displayVerification(row)}`,
    `Persisted to CRM: ${yesNo(row.persisted === true && row.path !== 'existing_crm')}`,
    `DNC state: ${row.dnc == null ? '(unknown)' : String(row.dnc)}`,
    `Eligible for CAPACITY projection: ${yesNo(isEligibleForCapacityProjection(row))}`,
  ];
  if (row.admissionReason) lines.splice(3, 0, `CRM admission: ${row.admissionReason}`);
  if (row.admissionBlocked) lines.splice(3, 0, `CRM admission blocked: ${row.admissionBlocked}`);
  return lines.join('\n');
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

  const { companyIds, rowsByCompanyId, admission } = await loadMissionBoundProspects(db, missionId, { dryRun });
  const admissionByCandidate = admission?.byCandidateId || new Map();
  const contacts = [];

  for (const companyId of companyIds) {
    const admissionResult = admissionByCandidate.get(String(companyId)) || null;
    const row = rowsByCompanyId.get(String(companyId));
    if (!row) {
      if (admissionResult?.blocked) {
        contacts.push({
          ...missingCrmResult(companyId),
          reason: admissionResult.reason || 'identity_admission_blocked',
          admissionBlocked: admissionResult.detail || admissionResult.reason,
        });
      } else {
        contacts.push(missingCrmResult(companyId));
      }
      continue;
    }
    const enriched = await enrichProspectRow(row, {
      db,
      dryRun,
      fetchDelayMs: options.fetchDelayMs,
    });
    contacts.push({
      ...enriched,
      crmCompanyId: row.company_id != null ? String(row.company_id) : admissionResult?.companyId || null,
      crmProspectUuid: row.prospect_id != null ? String(row.prospect_id) : admissionResult?.prospectId || null,
      admissionReason: admissionResult?.reason || null,
      admissionBlocked: admissionResult?.blocked ? (admissionResult.detail || admissionResult.reason) : null,
    });
  }

  const eligibleForCapacityProjection = contacts.filter(isEligibleForCapacityProjection).length;

  const report = {
    tenantId: TENANT_ID,
    clientId: CLIENT_ID,
    missionId,
    dryRun,
    missionBoundProspectIds: companyIds,
    missionBoundCompanyIds: companyIds,
    admission: admission?.results || [],
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

#!/usr/bin/env node
'use strict';

/**
 * Audit + remediate already-persisted email provenance for Anchor mission contacts.
 * Never sends mail. Never revises CAPACITY.
 *
 * Railway:
 *   node scripts/remediateAnchorEmailProvenance.js --confirm-production
 */

require('dotenv').config();

const pool = require('../db');
const { isProjectableCrmProspect } = require('../packages/max/workspace/MissionBoundCrmResolver');
const { ensureTieredEnrichmentSchema } = require('../utils/tieredEnrichmentSchema');
const {
  AUDIT_CONTACTS,
  remediateTaintedCrmEmail,
  snapshotRow: snapshotProvenanceRow,
} = require('../utils/crmEmailProvenance');
const {
  CLIENT_ID,
  DEFAULT_MISSION_ID,
  isExcludedCompany,
  loadProspectRow,
} = require('./lib/anchorMissionBoundEnrichment');

function parseArgs(argv = process.argv.slice(2)) {
  return {
    confirmProduction: argv.includes('--confirm-production'),
    dryRun: argv.includes('--dry-run'),
    help: argv.includes('--help') || argv.includes('-h'),
  };
}

function snapshotRow(row) {
  if (!row) return null;
  return snapshotProvenanceRow(row, {
    company: row.company_name,
    excluded: isExcludedCompany(row.company_name),
    projectable: isProjectableCrmProspect(row),
  });
}

async function auditAndRemediate(options = {}) {
  if (options.help) return { help: true };
  if (!options.confirmProduction) {
    throw Object.assign(new Error('Refusing to run without --confirm-production.'), {
      code: 'confirm_production_required',
    });
  }
  if (!process.env.DATABASE_URL) {
    throw Object.assign(new Error('Missing required runtime env: DATABASE_URL'), {
      code: 'runtime_env_missing',
    });
  }

  const db = options.pool || pool;
  await ensureTieredEnrichmentSchema();

  const contacts = [];
  for (const target of AUDIT_CONTACTS) {
    const beforeRow = await loadProspectRow(db, CLIENT_ID, target.prospectId);
    const before = snapshotRow(beforeRow);
    let remediation = { applied: false, reason: 'not_found_in_crm' };
    let after = before;
    if (beforeRow) {
      remediation = await remediateTaintedCrmEmail(db, beforeRow, { dryRun: options.dryRun });
      const afterRow = options.dryRun
        ? beforeRow
        : await loadProspectRow(db, CLIENT_ID, target.prospectId);
      after = snapshotRow(afterRow);
    }
    contacts.push({
      label: target.label,
      prospectId: target.prospectId,
      before,
      after,
      remediation: {
        action: remediation.plan?.action || null,
        reason: remediation.plan?.reason || remediation.reason || null,
        applied: remediation.applied === true,
        dryRun: remediation.dryRun === true,
      },
    });
  }

  const report = {
    tenantId: '10',
    clientId: CLIENT_ID,
    missionId: DEFAULT_MISSION_ID,
    dryRun: Boolean(options.dryRun),
    contacts,
    storesOriginalProvenanceSeparately: true,
    originalProvenanceField: 'enrichment_provenance.email.original_source',
    readPathLabel: 'existing_crm',
    regenerateCapacity: false,
    sendOutbound: false,
    completedAt: new Date().toISOString(),
  };
  if (options.print !== false) {
    console.log(JSON.stringify(report, null, 2));
  }
  return report;
}

module.exports = {
  AUDIT_CONTACTS,
  parseArgs,
  snapshotRow,
  auditAndRemediate,
};

if (require.main === module) {
  const options = parseArgs();
  auditAndRemediate(options)
    .then((report) => {
      if (report.help) {
        console.log('Usage: node scripts/remediateAnchorEmailProvenance.js --confirm-production [--dry-run]');
        return;
      }
      process.exitCode = 0;
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

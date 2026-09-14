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
const {
  configureScoringContext,
  runEnrichmentChain,
  resolveEmailVerification,
  normalizeDomain,
} = require('../leadgen');
const { resolveEnrichmentDomain } = require('../utils/websiteEnrichmentCrawl');
const tiered = require('../tieredEnrichmentAgent');
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
  const { rows } = await db.query(
    `SELECT
       p.id AS prospect_id,
       p.company_id,
       p.client_id,
       p.first_name,
       p.last_name,
       p.email,
       p.email_status,
       p.email_verified,
       p.email_verification_method,
       p.do_not_contact,
       p.notes,
       p.vertical,
       p.website_url,
       p.employee_count_estimate,
       p.practice_area,
       p.firm_size,
       p.enrichment_provenance,
       c.name AS company_name,
       c.website,
       c.domain,
       c.industry,
       c.size AS company_size,
       c.location,
       c.practice_area AS company_practice_area,
       c.firm_size AS company_firm_size
     FROM prospects p
     LEFT JOIN companies c
       ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND p.id::text = $2
    LIMIT 1`,
    [clientId, String(prospectId)]
  );
  return rows[0] || null;
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
  const prospectIds = listMissionBoundProspectIds(mission, snapshot.contributions || []);
  const rows = [];
  for (const prospectId of prospectIds) {
    const row = await loadProspectRow(db, CLIENT_ID, prospectId);
    if (row) rows.push(row);
  }
  return { mission, snapshot, prospectIds, rows };
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

  const domain = resolveEnrichmentDomain(row) || normalizeDomain(row.domain || row.website || row.website_url);
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

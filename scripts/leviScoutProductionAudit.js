#!/usr/bin/env node
'use strict';

// P0-SCOUT-LEVI-001: tenant-scoped, redacted, READ ONLY operational audit.
// It never calls a provider, mutates a schedule, imports a prospect or sends outreach.

const { TENANT_ID, evaluateProspect, weeklyProduction } = require('../services/leviScoutProductionContract');

const PLATFORMS = Object.freeze(['render', 'railway', 'local', 'other', 'unknown']);
const MIGRATION_SNAPSHOT_DATABASE = 'pulseforge_production_migration';
const ROLES = Object.freeze({
  RENDER_MIGRATION_SNAPSHOT: 'render_migration_snapshot_not_production',
  RAILWAY_LIVE_UNTIL_CUTOVER: 'railway_live_until_cutover',
  UNCLASSIFIED: 'unclassified',
});

const QUERIES = {
  tenant: `SELECT id,name,slug,vertical,active,enabled_agents,notes FROM clients WHERE id=$1`,
  // cie_interview_sessions has no blueprint columns (migrations/2026-08-06-client-intelligence-engine.sql);
  // Blueprint status comes from cie_business_blueprints. Only status/ids are emitted, never content.
  workspace: `SELECT id,status,current_stage,updated_at,
    (interview_state ? 'laEndeavorsWorkspaceCorrection') AS has_workspace_correction,
    interview_state->'growthInfrastructureReadinessReport'->>'overallStatus' AS readiness
    FROM cie_interview_sessions WHERE client_id=$1 ORDER BY updated_at DESC LIMIT 5`,
  blueprints: `SELECT id,version,status,generated_by,created_at,updated_at
    FROM cie_business_blueprints WHERE client_id=$1 ORDER BY updated_at DESC LIMIT 5`,
  runs: `SELECT agent_name,action,status,COALESCE(payload->>'reason',error_msg) AS reason,count(*)::int AS runs,
    max(ran_at) AS last_ran_at FROM agent_log WHERE client_id=$1 AND ran_at>=NOW()-INTERVAL '30 days'
    AND agent_name IN ('scout','scout_unenriched_enrichment','setter_handoff','handoff_utility','max_outbound_control')
    GROUP BY 1,2,3,4 ORDER BY last_ran_at DESC`,
  prospects: `SELECT p.id,p.created_at,p.source,p.status,p.job_title,p.email,p.phone,p.linkedin_url,p.notes,
    p.service_area_match,p.do_not_contact,p.acquisition_metadata,c.location AS company_location,
    EXISTS (SELECT 1 FROM prospects prior WHERE prior.client_id=p.client_id AND prior.id<>p.id
      AND (
        (p.email IS NOT NULL AND lower(prior.email)=lower(p.email)) OR
        (p.phone IS NOT NULL AND regexp_replace(prior.phone,'\\D','','g')=regexp_replace(p.phone,'\\D','','g')) OR
        (p.linkedin_url IS NOT NULL AND prior.linkedin_url=p.linkedin_url)
      ) AND (prior.created_at<p.created_at OR (prior.created_at=p.created_at AND prior.id::text<p.id::text))) AS duplicate_of
    FROM prospects p LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=$1 ORDER BY p.created_at DESC`,
  inventory: `SELECT source,status,COALESCE(acquisition_metadata->>'reviewState','unclassified') AS review_state,
    count(*)::int AS records,max(created_at) AS newest FROM prospects WHERE client_id=$1 GROUP BY 1,2,3 ORDER BY 4 DESC`,
};

// Platform-neutral target identity: the report states which database was
// read (name, server version, host class) without emitting the URL, host or
// credentials. Railway and Render are both recognised; neither is required.
function platformOf(url) {
  let host = '';
  try { host = new URL(String(url || '')).hostname.toLowerCase(); } catch (_) { return 'unknown'; }
  if (!host) return 'unknown';
  if (/(^|\.)(railway\.internal|rlwy\.net|railway\.app)$/.test(host)) return 'railway';
  if (/^dpg-[a-z0-9-]+$/.test(host) || /(^|\.)render\.com$/.test(host)) return 'render';
  if (['localhost', '127.0.0.1', '::1'].includes(host)) return 'local';
  return 'other';
}

function roleOf({ database, platform }) {
  if (database === MIGRATION_SNAPSHOT_DATABASE) return ROLES.RENDER_MIGRATION_SNAPSHOT;
  if (platform === 'railway') return ROLES.RAILWAY_LIVE_UNTIL_CUTOVER;
  return ROLES.UNCLASSIFIED;
}

function parseArgs(argv = []) {
  const out = { identityOnly: false };
  for (const arg of argv) {
    if (arg === '--identity-only') { out.identityOnly = true; continue; }
    const database = /^--expect-database=(.+)$/.exec(arg);
    if (database) { out.expectDatabase = database[1]; continue; }
    const platform = /^--expect-platform=(.+)$/.exec(arg);
    if (platform) {
      if (!PLATFORMS.includes(platform[1])) {
        throw Object.assign(new Error(`unknown argument: ${arg}`), { code: 'audit_argument_invalid' });
      }
      out.expectPlatform = platform[1];
      continue;
    }
    const role = /^--expect-role=(.+)$/.exec(arg);
    if (role) { out.expectRole = role[1]; continue; }
    throw Object.assign(new Error(`unknown argument: ${arg}`), { code: 'audit_argument_invalid' });
  }
  return out;
}

function assertTarget(target, { expectDatabase = null, expectPlatform = null, expectRole = null } = {}) {
  if (expectDatabase && target.database !== expectDatabase) {
    throw Object.assign(new Error('audit_target_database_mismatch'), { code: 'audit_target_database_mismatch' });
  }
  if (expectPlatform && target.platform !== expectPlatform) {
    throw Object.assign(new Error('audit_target_platform_mismatch'), { code: 'audit_target_platform_mismatch' });
  }
  if (expectRole && target.role !== expectRole) {
    throw Object.assign(new Error('audit_target_role_mismatch'), { code: 'audit_target_role_mismatch' });
  }
}

async function collect(pool, {
  now = new Date(),
  expectDatabase = null,
  expectPlatform = null,
  expectRole = null,
  identityOnly = false,
  databaseUrl = process.env.DATABASE_URL,
} = {}) {
  const client = await pool.connect();
  const report = { tenantId: TENANT_ID, generatedAt: now.toISOString(), readOnly: true, identityOnly: Boolean(identityOnly), sections: {} };
  try {
    await client.query('BEGIN TRANSACTION READ ONLY');
    await client.query("SET LOCAL statement_timeout='15s'");
    const identity = (await client.query(
      "SELECT current_database() AS database, current_setting('server_version_num')::int AS server_version_num")).rows[0];
    const platform = platformOf(databaseUrl);
    report.target = {
      database: identity.database,
      serverVersionNum: identity.server_version_num,
      platform,
      role: roleOf({ database: identity.database, platform }),
    };
    assertTarget(report.target, { expectDatabase, expectPlatform, expectRole });
    if (identityOnly) return report;
    for (const [name, sql] of Object.entries(QUERIES)) {
      await client.query(`SAVEPOINT audit_${name}`);
      try {
        report.sections[name] = (await client.query(sql, [TENANT_ID])).rows;
        await client.query(`RELEASE SAVEPOINT audit_${name}`);
      } catch (error) {
        await client.query(`ROLLBACK TO SAVEPOINT audit_${name}`);
        report.sections[name] = { unavailable: error.code || error.message };
      }
    }
    const all = Array.isArray(report.sections.prospects) ? report.sections.prospects : [];
    const since = now.getTime() - 30 * 86400000;
    const rows = all.filter(row => new Date(row.created_at).getTime() >= since);
    // The requested $5k-$20k range is intentionally not applied until the
    // canonical approved Blueprint supplies a machine-verifiable range.
    const summarize = list => {
      const evaluated = list.map(row => evaluateProspect(row));
      return {
        records: list.length,
        contractQualified: evaluated.filter(item => item.qualified).length,
        classifications: {
          contactNow: evaluated.filter(item => item.classification === 'Contact Now').length,
          qualifyFirst: evaluated.filter(item => item.classification === 'Qualify First').length,
          hold: evaluated.filter(item => item.classification === 'Hold').length,
        },
        duplicates: list.filter(row => row.duplicate_of).length,
        suppressed: list.filter(row => row.do_not_contact === true).length,
        bySource: list.reduce((out, row) => ({ ...out, [row.source || 'unknown']: (out[row.source || 'unknown'] || 0) + 1 }), {}),
        blockers: evaluated.flatMap(item => item.blockers).reduce((out, value) => ({ ...out, [value]: (out[value] || 0) + 1 }), {}),
      };
    };
    const reviewOnlyContactNow = all.filter(row => row.source === 'review_only_import'
      && String((row.acquisition_metadata && row.acquisition_metadata.reviewState) || '').toLowerCase() === 'contact_now');
    report.production = {
      contractStatus: 'proposed_range_requires_approved_blueprint_verification',
      observedProductionQualifiedNew: null,
      weekly: weeklyProduction(rows, now),
      last30Days: { discovered: rows.length, contractQualifiedCandidates: summarize(rows).contractQualified, ...summarize(rows) },
      allTime: summarize(all),
      reviewOnlyContactNow: summarize(reviewOnlyContactNow),
    };
    // Never emit borrower rows or contact/source values.
    delete report.sections.prospects;
    return report;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
}

module.exports = {
  QUERIES, collect, platformOf, parseArgs, roleOf, assertTarget,
  PLATFORMS, MIGRATION_SNAPSHOT_DATABASE, ROLES,
};

if (require.main === module) {
  Promise.resolve().then(() => collect(require('../db'), parseArgs(process.argv.slice(2))))
    .then(report => { console.log(JSON.stringify(report, null, 2)); process.exit(0); })
    .catch(error => { console.error(`[levi-scout-production-audit] ${error.message}`); process.exit(2); });
}

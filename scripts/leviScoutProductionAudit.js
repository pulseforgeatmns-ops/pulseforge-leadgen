#!/usr/bin/env node
'use strict';

// P0-SCOUT-LEVI-001: tenant-scoped, redacted, READ ONLY operational audit.
// It never calls a provider, mutates a schedule, imports a prospect or sends outreach.

const { TENANT_ID, evaluateProspect, weeklyProduction } = require('../services/leviScoutProductionContract');

const QUERIES = {
  tenant: `SELECT id,name,slug,vertical,active,enabled_agents,notes FROM clients WHERE id=$1`,
  workspace: `SELECT id,status,blueprint_id,blueprint_version,interview_state->'laEndeavorsWorkspaceCorrection' AS correction,
    interview_state->'growthInfrastructureReadinessReport'->>'overallStatus' AS readiness
    FROM cie_interview_sessions WHERE client_id=$1 ORDER BY updated_at DESC LIMIT 5`,
  runs: `SELECT agent_name,action,status,COALESCE(payload->>'reason',error_msg) AS reason,count(*)::int AS runs,
    max(ran_at) AS last_ran_at FROM agent_log WHERE client_id=$1 AND ran_at>=NOW()-INTERVAL '30 days'
    AND agent_name IN ('scout','scout_unenriched_enrichment','setter_handoff','handoff_utility','max_outbound_control')
    GROUP BY 1,2,3,4 ORDER BY last_ran_at DESC`,
  prospects: `SELECT p.id,p.created_at,p.source,p.status,p.job_title,p.email,p.phone,p.linkedin_url,p.notes,
    p.service_area_match,p.do_not_contact,p.acquisition_metadata,c.location AS company_location,
    EXISTS (SELECT 1 FROM prospects prior WHERE prior.client_id=p.client_id AND prior.id<>p.id
      AND prior.created_at<=p.created_at AND (
        (p.email IS NOT NULL AND lower(prior.email)=lower(p.email)) OR
        (p.phone IS NOT NULL AND regexp_replace(prior.phone,'\\D','','g')=regexp_replace(p.phone,'\\D','','g')) OR
        (p.linkedin_url IS NOT NULL AND prior.linkedin_url=p.linkedin_url)
      )) AS duplicate_of
    FROM prospects p LEFT JOIN companies c ON c.id=p.company_id AND c.client_id=p.client_id
    WHERE p.client_id=$1 AND p.created_at>=NOW()-INTERVAL '30 days' ORDER BY p.created_at DESC`,
  inventory: `SELECT source,status,COALESCE(acquisition_metadata->>'reviewState','unclassified') AS review_state,
    count(*)::int AS records,max(created_at) AS newest FROM prospects WHERE client_id=$1 GROUP BY 1,2,3 ORDER BY 4 DESC`,
};

async function collect(pool, { now = new Date() } = {}) {
  const client = await pool.connect();
  const report = { tenantId: TENANT_ID, generatedAt: now.toISOString(), readOnly: true, sections: {} };
  try {
    await client.query('BEGIN TRANSACTION READ ONLY');
    await client.query("SET LOCAL statement_timeout='15s'");
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
    const rows = Array.isArray(report.sections.prospects) ? report.sections.prospects : [];
    const evaluated = rows.map(evaluateProspect);
    report.production = {
      contractStatus: 'proposed_range_requires_approved_blueprint_verification',
      observedProductionQualifiedNew: null,
      weekly: weeklyProduction(rows, now),
      last30Days: {
        discovered: rows.length,
        contractQualifiedCandidates: evaluated.filter(item => item.qualified).length,
        classifications: {
          contactNow: evaluated.filter(item => item.classification === 'Contact Now').length,
          qualifyFirst: evaluated.filter(item => item.classification === 'Qualify First').length,
          hold: evaluated.filter(item => item.classification === 'Hold').length,
        },
        blockers: evaluated.flatMap(item => item.blockers).reduce((out, value) => ({ ...out, [value]: (out[value] || 0) + 1 }), {}),
      },
    };
    // Never emit borrower rows or contact/source values.
    delete report.sections.prospects;
    return report;
  } finally {
    await client.query('ROLLBACK').catch(() => {});
    client.release();
  }
}

module.exports = { QUERIES, collect };

if (require.main === module) {
  collect(require('../db'))
    .then(report => { console.log(JSON.stringify(report, null, 2)); process.exit(0); })
    .catch(error => { console.error(`[levi-scout-production-audit] ${error.message}`); process.exit(2); });
}

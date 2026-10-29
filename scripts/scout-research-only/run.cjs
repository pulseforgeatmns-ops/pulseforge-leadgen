'use strict';
// SPEC-SCOUT-P0-003 operator CLI: read the research-only grant receipt, or
// revoke it. Installation is deliberately not here: it needs a live admin
// session (POST /api/v1/tenant-outreach/anchor-program/research-only-grant/install).
// No app bootstrap, provider, discovery, preparation or send path is loaded.
//
//   node scripts/scout-research-only/run.cjs --inspect [grantId]
//   node scripts/scout-research-only/run.cjs --revoke <grantId> --approval-ref <ref> --confirm <grantId>
//   node scripts/scout-research-only/run.cjs --reconcile-plan <researchMissionId>
//   node scripts/scout-research-only/run.cjs --reconcile-apply <researchMissionId> --approval-ref <ref> --confirm <planHash>
// Reconciliation only queues retained research candidates for ordinary Scout
// enrichment under an authorized mission with identical targeting, after
// recovery maintenance has ended and the research grant is closed.
const { Pool } = require('pg');

function invocation(args) {
  const hex = value => /^[a-f0-9]{64}$/.test(value || '');
  if (args[0] === '--inspect' && (args.length === 1 || (args.length === 2 && hex(args[1])))) return { mode: 'inspect', grantId: args[1] };
  if (args[0] === '--revoke' && args.length === 6 && hex(args[1]) && args[2] === '--approval-ref'
    && /^[a-zA-Z0-9_.:-]{1,120}$/.test(args[3]) && args[4] === '--confirm' && args[5] === args[1]) {
    return { mode: 'revoke', grantId: args[1], approvalReference: args[3] };
  }
  const mission = value => /^mission_anchor_facility_research_[a-f0-9]{20}$/.test(value || '');
  if (args[0] === '--reconcile-plan' && args.length === 2 && mission(args[1])) return { mode: 'reconcile-plan', researchMissionId: args[1] };
  if (args[0] === '--reconcile-apply' && args.length === 6 && mission(args[1]) && args[2] === '--approval-ref'
    && /^[a-zA-Z0-9_.:-]{1,120}$/.test(args[3]) && args[4] === '--confirm' && hex(args[5])) {
    return { mode: 'reconcile-apply', researchMissionId: args[1], approvalRef: args[3], confirm: args[5] };
  }
  throw Object.assign(Error('research_only_explicit_invocation_required'), { code: 'research_only_explicit_invocation_required' });
}

async function main(args = process.argv.slice(2), env = process.env) {
  const request = invocation(args);
  if (!env.DATABASE_URL) throw Object.assign(Error('research_only_database_required'), { code: 'research_only_database_required' });
  const pool = new Pool({ connectionString: env.DATABASE_URL, max: 2, connectionTimeoutMillis: 5000,
    ssl: env.DATABASE_SSL === 'false' ? false : (env.DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false }) });
  try {
    const grants = require('../../services/scoutResearchOnlyGrant');
    const missions = require('../../services/scoutResearchMission');
    if (request.mode === 'reconcile-plan') return await missions.previewReconciliation(pool, { researchMissionId: request.researchMissionId });
    if (request.mode === 'reconcile-apply') return await missions.applyReconciliation(pool, request);
    return request.mode === 'inspect'
      ? await grants.inspectResearchOnlyGrant(pool, request.grantId)
      : await grants.revokeResearchOnlyGrant(pool, { grantId: request.grantId, approvalReference: request.approvalReference },
        { kind: 'operator_cli' });
  } finally { await pool.end(); }
}

if (require.main === module) {
  main().then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`))
    .catch(error => {
      process.stderr.write(`${/^research_(only|mission|reconcile)_[a-z_]+$/.test(error.code || '') ? error.code : 'research_only_stopped'}\n`);
      process.exitCode = 1;
    });
}
module.exports = { invocation, main };

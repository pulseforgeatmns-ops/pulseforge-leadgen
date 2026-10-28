'use strict';
// SPEC-SCOUT-P0-003 operator CLI: read the research-only grant receipt, or
// revoke it. Installation is deliberately not here: it needs a live admin
// session (POST /api/v1/tenant-outreach/anchor-program/research-only-grant/install).
// No app bootstrap, provider, discovery, preparation or send path is loaded.
//
//   node scripts/scout-research-only/run.cjs --inspect [grantId]
//   node scripts/scout-research-only/run.cjs --revoke <grantId> --approval-ref <ref> --confirm <grantId>
const { Pool } = require('pg');

function invocation(args) {
  const hex = value => /^[a-f0-9]{64}$/.test(value || '');
  if (args[0] === '--inspect' && (args.length === 1 || (args.length === 2 && hex(args[1])))) return { mode: 'inspect', grantId: args[1] };
  if (args[0] === '--revoke' && args.length === 6 && hex(args[1]) && args[2] === '--approval-ref'
    && /^[a-zA-Z0-9_.:-]{1,120}$/.test(args[3]) && args[4] === '--confirm' && args[5] === args[1]) {
    return { mode: 'revoke', grantId: args[1], approvalReference: args[3] };
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
    return request.mode === 'inspect'
      ? await grants.inspectResearchOnlyGrant(pool, request.grantId)
      : await grants.revokeResearchOnlyGrant(pool, { grantId: request.grantId, approvalReference: request.approvalReference },
        { kind: 'operator_cli' });
  } finally { await pool.end(); }
}

if (require.main === module) {
  main().then(result => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`))
    .catch(error => {
      process.stderr.write(`${/^research_only_[a-z_]+$/.test(error.code || '') ? error.code : 'research_only_stopped'}\n`);
      process.exitCode = 1;
    });
}
module.exports = { invocation, main };

'use strict';

async function run(options = {}) {
  return require('./services/governedOutbound').productionService(options.pool).tick();
}
async function poll(options = {}) {
  const pool = options.pool || require('./db');
  // Continue receiving replies after a pause, expiry or revocation, including
  // older inboxes if a later grant changes the bound integration.
  const programs = (await pool.query(`SELECT DISTINCT ON (policy->>'inboxIntegrationId') *
    FROM acquisition_outbound_programs WHERE tenant_id='10'
    ORDER BY policy->>'inboxIntegrationId',authorized_at DESC`)).rows;
  if (!programs.length) return { halted: 'no_program' };
  const { PostgresTenantMailboxStore } = require('./services/tenantMailbox');
  const results = [];
  for (const program of programs) {
    const integration = await new PostgresTenantMailboxStore(pool).getIntegration('10', program.policy.inboxIntegrationId);
    if (!integration || integration.mailboxAddress.toLowerCase() !== program.policy.senderEmail) {
      results.push({ halted: 'inbox_identity_mismatch', programId: program.id }); continue;
    }
    results.push(await require('./services/tenantMailboxPollExecutor').pollOneIntegration(integration, { ...options, pool }));
  }
  const classification = await require('./services/governedOutboundReplies').classifyPending(pool, options);
  return { results, classification };
}
module.exports = { run, poll };

'use strict';

async function run(options = {}) {
  return require('./services/governedOutbound').productionService(options.pool).tick();
}
const ANCHOR_DEFAULT_INBOX_INTEGRATION_ID = 'tmi_10_anchor_jacob';

async function pollAnchorMailboxOnly(pool, options = {}) {
  const { PostgresTenantMailboxStore } = require('./services/tenantMailbox');
  const store = options.store || new PostgresTenantMailboxStore(pool);
  const integrationId = options.integrationId
    || process.env.ANCHOR_INBOX_INTEGRATION_ID
    || ANCHOR_DEFAULT_INBOX_INTEGRATION_ID;
  const integration = await store.getIntegration('10', integrationId);
  if (!integration) {
    return {
      halted: 'anchor_mailbox_missing',
      integrationId,
      results: [],
      classification: { classified: 0, skipped: 'no_mailbox' },
    };
  }
  const pollResult = await require('./services/tenantMailboxPollExecutor').pollOneIntegration(integration, {
    ...options,
    pool,
    store,
    mailboxStore: store,
  });
  return {
    mailboxOnly: true,
    integrationId,
    results: [pollResult],
    classification: { classified: 0, skipped: 'no_program' },
  };
}

async function poll(options = {}) {
  const pool = options.pool || require('./db');
  // Continue receiving replies after a pause, expiry or revocation, including
  // older inboxes if a later grant changes the bound integration.
  const programs = (await pool.query(`SELECT DISTINCT ON (policy->>'inboxIntegrationId') *
    FROM acquisition_outbound_programs WHERE tenant_id='10'
    ORDER BY policy->>'inboxIntegrationId',authorized_at DESC`)).rows;
  if (!programs.length) {
    if (options.mailboxOnly === false) return { halted: 'no_program' };
    return pollAnchorMailboxOnly(pool, options);
  }
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
module.exports = { run, poll, pollAnchorMailboxOnly, ANCHOR_DEFAULT_INBOX_INTEGRATION_ID };

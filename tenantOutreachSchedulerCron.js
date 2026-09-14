'use strict';

/**
 * SPEC-252 — Railway cron adapter for tenant outreach scheduled send executor.
 *
 * POST /cron/tenant-outreach-executor?secret={CRON_SECRET}
 */

require('dotenv').config();

const { executeDueScheduledSends } = require('./services/tenantOutreachScheduler');

async function run(options = {}) {
  return executeDueScheduledSends({
    limit: options.limit || options.max || 20,
    now: options.now,
    pool: options.pool,
    scheduleStore: options.scheduleStore,
    mailboxStore: options.mailboxStore,
    globalEnabled: options.globalEnabled,
  });
}

module.exports = { run };

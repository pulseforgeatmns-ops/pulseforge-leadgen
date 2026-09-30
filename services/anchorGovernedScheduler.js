'use strict';

const { startGovernedOutboundExecutionClock } = require('./governedOutboundExecutionClock');

// Backward-compatible entry: Anchor historically started an in-process scheduler
// for tenant 10 only. Mailbox tenants (13, 17) stay on external cron ticks.
function startAnchorGovernedScheduler(options = {}) {
  return startGovernedOutboundExecutionClock({
    ...options,
    enabled: options.enabled ?? process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED === 'true',
    tenantIds: options.tenantIds ?? ['10'],
  });
}

module.exports = { startAnchorGovernedScheduler, startGovernedOutboundExecutionClock };

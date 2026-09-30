'use strict';

const { startGovernedOutboundExecutionClock } = require('./governedOutboundExecutionClock');

// Backward-compatible entry: Anchor historically started an in-process scheduler
// for tenant 10 only. The governed execution clock now ticks every configured
// tenant in GOVERNED_OUTBOUND_TENANT_IDS when armed.
function startAnchorGovernedScheduler(options = {}) {
  return startGovernedOutboundExecutionClock(options);
}

module.exports = { startAnchorGovernedScheduler, startGovernedOutboundExecutionClock };

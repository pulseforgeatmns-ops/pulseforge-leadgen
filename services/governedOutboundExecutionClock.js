'use strict';

const {
  parseGovernedOutboundTenantIds,
  governedOutboundEnabledForTenant,
} = require('./governedOutboundTenant');

/**
 * Recurring governed send clock: evaluates pending acquisition_outbound_items via
 * governedOutbound.tick() for each configured tenant. Preparation (Max control) is
 * intentionally separate; this module owns dispatch/scheduling attempts only.
 */

function executionClockEnabled(options = {}) {
  if (options.enabled != null) return Boolean(options.enabled);
  if (process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED === 'false') return false;
  if (process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED === 'true') return true;
  if (process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED === 'true') return true;
  // Mailbox tenants (13, 17) have no Anchor-only in-process scheduler; arm the
  // tick clock whenever live governed sending is enabled for a non-Anchor tenant.
  return parseGovernedOutboundTenantIds().some((tenantId) => tenantId !== '10'
    && governedOutboundEnabledForTenant(tenantId));
}

function resolveTickTenantIds(options = {}) {
  const configured = options.tenantIds || parseGovernedOutboundTenantIds();
  const unique = [...new Set(configured.map(String))];
  if (options.enabledTenantsOnly !== true) return unique;
  return unique.filter((tenantId) => governedOutboundEnabledForTenant(tenantId));
}

async function runGovernedOutboundTickCycle(options = {}) {
  const pool = options.pool || require('../db');
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const tenantIds = resolveTickTenantIds(options);
  if (!tenantIds.length) {
    return { tenants: {}, skipped: 'no_enabled_governed_tenants' };
  }
  if (tenantIds.length === 1 && !options.allTenants) {
    const single = await cron.run({ pool, tenantIds, allTenants: true });
    return { tenants: { [tenantIds[0]]: single } };
  }
  return cron.run({ pool, tenantIds, allTenants: true });
}

async function runGovernedOutboundPollCycle(options = {}) {
  const pool = options.pool || require('../db');
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const tenantIds = options.tenantIds || parseGovernedOutboundTenantIds();
  return cron.poll({ pool, tenantIds, ...options });
}

function startGovernedOutboundExecutionClock(options = {}) {
  if (!executionClockEnabled(options)) return null;
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const logger = options.logger || console;
  const schedule = options.setInterval || setInterval;
  const cancel = options.clearInterval || clearInterval;
  const pool = options.pool || require('../db');
  const maxControlEnabled = options.maxControlEnabled
    ?? process.env.ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED === 'true';
  const dispatchControl = options.dispatchControl
    || ((opts) => require('./governedOutboundControlDispatch').dispatchGovernedOutboundControl(opts));
  let busy = false;
  let stopped = false;
  let cycles = 0;

  async function cycle() {
    if (busy || stopped) return;
    busy = true;
    const current = cycles++;
    try {
      const tenantIds = parseGovernedOutboundTenantIds();
      await runGovernedOutboundPollCycle({ pool, cron, tenantIds, ...options });
      if (current % 5 === 0) {
        const tick = await runGovernedOutboundTickCycle({ pool, cron, tenantIds, allTenants: true });
        logger.log?.('[governed-outbound-clock] tick', JSON.stringify(tick));
      }
      if (maxControlEnabled && current % 15 === 0) {
        const control = await dispatchControl({ pool, logger, execute: true, tenantIds });
        logger.log?.('[governed-outbound-clock] max-control', JSON.stringify(control));
      }
    } catch (error) {
      logger.error?.('[governed-outbound-clock]', error.code || error.message);
    } finally {
      busy = false;
    }
  }

  const timer = schedule(cycle, 60000);
  timer.unref?.();
  if (options.autostart !== false) void cycle();
  return { cycle, stop() { stopped = true; cancel(timer); } };
}

module.exports = {
  executionClockEnabled,
  resolveTickTenantIds,
  runGovernedOutboundTickCycle,
  runGovernedOutboundPollCycle,
  startGovernedOutboundExecutionClock,
};

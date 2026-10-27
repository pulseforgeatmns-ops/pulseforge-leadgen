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
  // Mailbox tenants (1, 13, 17) have no Anchor-only in-process scheduler; arm the
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
  const durable = require('./governedOutboundStageScheduler');
  if (durable.durableSchedulingEnabled()) return durable.dispatchStages({ ...options, tenantIds: resolveTickTenantIds(options), stages: ['tick'] });
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
  const durable = require('./governedOutboundStageScheduler');
  if (durable.durableSchedulingEnabled()) return durable.dispatchStages({ ...options, tenantIds: resolveTickTenantIds(options), stages: ['poll'] });
  const pool = options.pool || require('../db');
  const cron = options.cron || require('../anchorDailyOutboundCron');
  const tenantIds = options.tenantIds || parseGovernedOutboundTenantIds();
  return cron.poll({ pool, tenantIds, ...options });
}

function startGovernedOutboundExecutionClock(options = {}) {
  if (!executionClockEnabled(options)) return null;
  const durable = require('./governedOutboundStageScheduler');
  // Validate before either timer implementation; global mode must not hide an
  // invalid scoped rollout value that the external dispatcher would reject.
  durable.scopedDurableControlTenantIds(resolveTickTenantIds(options));
  if (durable.durableSchedulingEnabled()) return durable.startDurableClock({ ...options, tenantIds: resolveTickTenantIds(options), maxControlEnabled: options.maxControlEnabled ?? process.env.ANCHOR_MAX_OUTBOUND_CONTROL_ENABLED === 'true' });
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
  let scopedAdmission = null;

  async function cycle() {
    if (stopped) return;
    let tenantIds, scoped;
    try {
      tenantIds = resolveTickTenantIds(options);
      scoped = maxControlEnabled ? durable.scopedDurableControlTenantIds(tenantIds) : [];
      if (scoped.length && options.maxControl) throw Object.assign(Error('scoped_control_override_not_allowed'), { code: 'scoped_control_override_not_allowed' });
      // Scan durable control each minute, even if an earlier poll is hung. This
      // only requests bounded admission; PostgreSQL due_at owns the 15m cadence.
      // One outstanding admission acknowledgement per clock prevents unbounded
      // promise growth, and the stage runner separately bounds child ownership.
      if (scoped.length && !scopedAdmission) {
        scopedAdmission = Promise.resolve(dispatchControl({ pool, logger, execute: true, tenantIds: scoped }))
          .then(result => logger.log?.('[governed-outbound-clock] durable-control-request', JSON.stringify(result)))
          .catch(error => logger.error?.('[governed-outbound-clock]', error.code || error.message))
          .finally(() => { scopedAdmission = null; });
      }
    } catch (error) {
      logger.error?.('[governed-outbound-clock]', error.code || error.message);
      return; // Invalid scoped configuration never admits legacy control.
    }
    if (busy || stopped) return;
    busy = true;
    const current = cycles++;
    try {
      await runGovernedOutboundPollCycle({ ...options, pool, cron, tenantIds });
      if (current % 5 === 0) {
        const tick = await runGovernedOutboundTickCycle({
          ...options,
          pool,
          cron,
          tenantIds,
          allTenants: true,
        });
        logger.log?.('[governed-outbound-clock] tick', JSON.stringify(tick));
      }
      const legacyControlTenants = tenantIds.filter(tenantId => !scoped.includes(tenantId));
      if (maxControlEnabled && current % 15 === 0 && legacyControlTenants.length) {
        const control = options.maxControl
          ? await options.maxControl()
          : await dispatchControl({ pool, logger, execute: true, tenantIds: legacyControlTenants });
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

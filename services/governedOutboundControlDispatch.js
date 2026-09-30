'use strict';

const crypto = require('crypto');
const {
  GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE,
  assertGovernedOutboundTenantId,
  createGovernedOutboundTenantContext,
  parseGovernedOutboundTenantIds,
} = require('./governedOutboundTenant');

// In-process reservation plus a session advisory lock. The reservation closes
// the gap before the database lock returns; the advisory lock covers another
// worker. The lock namespace is not the send/tick namespace, so Scout work
// does not take Emmett's lock.
const cycles = new Map();
const background = new Set();

function defaultRunControl(options) {
  return require('./maxOutboundControlLoop').runMaxOutboundControlLoop(options);
}

function resolveTenantIds({ tenantId = null, tenantIds = null } = {}) {
  if (tenantId != null && String(tenantId).trim()) {
    return [assertGovernedOutboundTenantId(tenantId)];
  }
  if (Array.isArray(tenantIds) && tenantIds.length) {
    return [...new Set(tenantIds.map(assertGovernedOutboundTenantId))];
  }
  return parseGovernedOutboundTenantIds();
}

function publicTenantStatus({ tenantId, status, cycleId, reason, timestamp }) {
  return {
    tenant: String(tenantId),
    status,
    cycleId: cycleId || null,
    reason: reason || null,
    timestamp,
  };
}

function track(promise) {
  const tracked = Promise.resolve(promise).finally(() => {
    background.delete(tracked);
  });
  background.add(tracked);
  return tracked;
}

async function whenControlDispatchSettled() {
  while (background.size) {
    await Promise.allSettled([...background]);
  }
}

function resetControlDispatchForTests() {
  cycles.clear();
  background.clear();
}

async function admitControlCycle(pool, tenantId) {
  const tid = assertGovernedOutboundTenantId(tenantId);
  if (cycles.has(tid)) return { admitted: false, reason: 'overlap', cycleId: null };
  const ctx = createGovernedOutboundTenantContext(tid);
  const cycleId = `moc_${tid}_${crypto.randomBytes(8).toString('hex')}`;
  cycles.set(tid, { cycleId });
  let client = null;
  try {
    client = await pool.connect();
    const locked = (await client.query(
      'SELECT pg_try_advisory_lock($1,$2) AS locked',
      [ctx.controlLockNamespace, ctx.advisoryLockKey]
    )).rows[0]?.locked === true;
    if (!locked) {
      cycles.delete(tid);
      client.release();
      return { admitted: false, reason: 'overlap', cycleId: null };
    }
    let released = false;
    const release = async () => {
      if (released) return;
      released = true;
      cycles.delete(tid);
      try {
        await client.query(
          'SELECT pg_advisory_unlock($1,$2)',
          [ctx.controlLockNamespace, ctx.advisoryLockKey]
        );
      } finally {
        client.release();
      }
    };
    return { admitted: true, reason: null, cycleId, release };
  } catch (error) {
    cycles.delete(tid);
    if (client) {
      try { client.release(); } catch (_) { /* connection already closed */ }
    }
    throw error;
  }
}

function launchControlCycle({
  pool,
  tenantId,
  cycleId,
  execute,
  logger,
  runControl,
  release,
}) {
  const launched = (async () => {
    try {
      const result = await runControl({
        pool,
        execute,
        logger,
        tenantId,
        cycleId,
      });
      logger.log?.('[max-outbound-control] cycle finished', JSON.stringify({
        tenant: tenantId,
        cycleId,
        status: 'finished',
        halted: result?.halted || null,
        state: result?.plan?.state || null,
      }));
      return result;
    } catch (error) {
      logger.error?.('[max-outbound-control] tenant cycle failed', JSON.stringify({
        tenant: tenantId,
        cycleId,
        reason: error.code || error.message,
      }));
      return null;
    } finally {
      try {
        await release();
      } catch (error) {
        logger.error?.('[max-outbound-control] control lock release failed', JSON.stringify({
          tenant: tenantId,
          cycleId,
          reason: error.code || error.message,
        }));
      }
    }
  })();
  return track(launched);
}

async function dispatchGovernedOutboundControl(options = {}) {
  const pool = options.pool || require('../db');
  const logger = options.logger || console;
  const execute = options.execute !== false;
  const runControl = options.runControl || defaultRunControl;
  const tenantIds = resolveTenantIds(options);
  const dispatchedAt = new Date().toISOString();
  const tenants = {};

  await Promise.all(tenantIds.map(async (tenantId) => {
    const timestamp = new Date().toISOString();
    try {
      const admission = await admitControlCycle(pool, tenantId);
      if (!admission.admitted) {
        tenants[tenantId] = publicTenantStatus({
          tenantId,
          status: 'skipped',
          cycleId: null,
          reason: admission.reason || 'overlap',
          timestamp,
        });
        logger.log?.('[max-outbound-control] dispatched', JSON.stringify(tenants[tenantId]));
        return;
      }
      // Start the cycle now. Do not wait for Scout or the rest of the loop:
      // another tenant must be able to begin inside this same tick.
      launchControlCycle({
        pool,
        tenantId,
        cycleId: admission.cycleId,
        execute,
        logger,
        runControl,
        release: admission.release,
      });
      tenants[tenantId] = publicTenantStatus({
        tenantId,
        status: 'started',
        cycleId: admission.cycleId,
        reason: null,
        timestamp,
      });
      logger.log?.('[max-outbound-control] dispatched', JSON.stringify(tenants[tenantId]));
    } catch (error) {
      tenants[tenantId] = publicTenantStatus({
        tenantId,
        status: 'skipped',
        cycleId: null,
        reason: error.code || 'dispatch_failed',
        timestamp,
      });
      logger.error?.('[max-outbound-control] dispatch failed', JSON.stringify(tenants[tenantId]));
    }
  }));

  return {
    bounded: true,
    dispatchedAt,
    execute,
    tenants,
  };
}

async function runBoundedMaxOutboundControl(options = {}) {
  const pool = options.pool || require('../db');
  const tenantId = require('./governedOutboundContext').assertGovernedOutboundTenantRequired(options.tenantId);
  const logger = options.logger || console;
  const runControl = options.runControl || defaultRunControl;
  const admission = await admitControlCycle(pool, tenantId);
  const timestamp = new Date().toISOString();
  if (!admission.admitted) {
    return {
      halted: 'overlap',
      ...publicTenantStatus({
        tenantId,
        status: 'skipped',
        cycleId: null,
        reason: admission.reason || 'overlap',
        timestamp,
      }),
    };
  }
  try {
    return await runControl({
      ...options,
      pool,
      tenantId,
      cycleId: admission.cycleId,
      logger,
    });
  } finally {
    await admission.release();
  }
}

module.exports = {
  GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE,
  dispatchGovernedOutboundControl,
  runBoundedMaxOutboundControl,
  whenControlDispatchSettled,
  resetControlDispatchForTests,
  _test: {
    admitControlCycle,
    resolveTenantIds,
    publicTenantStatus,
  },
};

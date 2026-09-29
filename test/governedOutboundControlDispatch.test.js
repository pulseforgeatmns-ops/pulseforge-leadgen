'use strict';

const { describe, test, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');

const {
  GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE,
  GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE,
  createGovernedOutboundTenantContext,
} = require('../services/governedOutboundTenant');
const {
  dispatchGovernedOutboundControl,
  whenControlDispatchSettled,
  resetControlDispatchForTests,
} = require('../services/governedOutboundControlDispatch');

function memoryPool() {
  const locks = new Set();
  const queries = [];
  return {
    queries,
    async connect() {
      return {
        async query(sql, args) {
          queries.push({ sql, args: args ? [...args] : [] });
          if (sql.includes('pg_try_advisory_lock')) {
            const key = `${args[0]}:${args[1]}`;
            if (locks.has(key)) return { rows: [{ locked: false }] };
            locks.add(key);
            return { rows: [{ locked: true }] };
          }
          if (sql.includes('pg_advisory_unlock')) {
            locks.delete(`${args[0]}:${args[1]}`);
            return { rows: [] };
          }
          throw new Error(`unexpected sql: ${sql}`);
        },
        release() {},
      };
    },
  };
}

function listen(app) {
  const server = app.listen(0, '127.0.0.1');
  return new Promise((resolve) => {
    server.on('listening', () => {
      const { port } = server.address();
      resolve({
        base: `http://127.0.0.1:${port}`,
        async close() {
          await new Promise((r) => server.close(r));
        },
      });
    });
  });
}

describe('bounded governed outbound control dispatch', { concurrency: 1 }, () => {
beforeEach(() => {
  resetControlDispatchForTests();
});

afterEach(async () => {
  await whenControlDispatchSettled();
  resetControlDispatchForTests();
});

test('control lock is per tenant and is not the send lock', () => {
  const anchor = createGovernedOutboundTenantContext('10');
  const babrun = createGovernedOutboundTenantContext('13');
  assert.equal(anchor.controlLockNamespace, GOVERNED_OUTBOUND_CONTROL_LOCK_NAMESPACE);
  assert.equal(anchor.controlLockNamespace, 261020);
  assert.equal(anchor.advisoryLockNamespace, GOVERNED_OUTBOUND_SEND_LOCK_NAMESPACE);
  assert.notEqual(anchor.controlLockNamespace, anchor.advisoryLockNamespace);
  assert.notEqual(anchor.advisoryLockKey, babrun.advisoryLockKey);
});

test('a slow tenant does not delay another tenant starting its control cycle', async () => {
  let releaseSlow;
  const slowGate = new Promise((resolve) => { releaseSlow = resolve; });
  const events = [];
  const effects = [];
  const pool = memoryPool();
  const logger = { log() {}, error() {} };

  const startedAt = Date.now();
  const result = await dispatchGovernedOutboundControl({
    pool,
    tenantIds: ['10', '13'],
    logger,
    runControl: async ({ tenantId, cycleId }) => {
      events.push(`start:${tenantId}`);
      effects.push({ tenantId, cycleId, effect: 'schedule' });
      effects.push({ tenantId, cycleId, effect: 'grant' });
      if (tenantId === '10') {
        events.push('scout:10:blocked');
        await slowGate;
        events.push('scout:10:done');
        return { plan: { state: 'replenish' } };
      }
      events.push('scout:13:done');
      return { plan: { state: 'healthy' } };
    },
  });

  assert.ok(Date.now() - startedAt < 500);
  assert.equal(result.bounded, true);
  assert.equal(result.tenants['10'].status, 'started');
  assert.equal(result.tenants['13'].status, 'started');
  assert.ok(result.tenants['10'].cycleId);
  assert.ok(result.tenants['13'].cycleId);
  assert.notEqual(result.tenants['10'].cycleId, result.tenants['13'].cycleId);
  assert.equal(result.tenants['10'].reason, null);
  assert.equal(result.tenants['13'].reason, null);
  assert.ok(events.includes('start:13'));
  assert.ok(events.includes('scout:13:done'));
  assert.equal(events.includes('scout:10:done'), false);
  assert.ok(events.indexOf('start:13') < events.indexOf('scout:10:done') || !events.includes('scout:10:done'));

  const body = JSON.stringify(result);
  for (const leaked of ['scout', 'discovery', 'yield', 'lossBuckets', 'funnel', 'companies', 'prospects']) {
    assert.equal(body.includes(leaked), false, `response leaked ${leaked}`);
  }
  assert.ok(body.length < 2000);
  assert.deepEqual(
    pool.queries.filter((query) => query.sql.includes('pg_try_advisory_lock')).map((query) => query.args),
    [[261020, 10], [261020, 13]]
  );
  assert.equal(pool.queries.some((query) => query.args[0] === 261018), false);

  releaseSlow();
  await whenControlDispatchSettled();
  assert.ok(events.includes('scout:10:done'));
  assert.equal(effects.filter((row) => row.effect === 'schedule').length, 2);
  assert.equal(effects.filter((row) => row.effect === 'grant').length, 2);
  const tenantsTouched = new Set(effects.map((row) => row.tenantId));
  assert.deepEqual([...tenantsTouched].sort(), ['10', '13']);
});

test('an overlapping tick for the same tenant is skipped and does not repeat side effects', async () => {
  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const calls = [];
  const pool = memoryPool();
  const logger = { log() {}, error() {} };
  const runControl = async ({ tenantId }) => {
    calls.push(tenantId);
    await gate;
    return { plan: { state: 'replenish' } };
  };

  const first = await dispatchGovernedOutboundControl({
    pool,
    tenantIds: ['10', '13'],
    logger,
    runControl,
  });
  const second = await dispatchGovernedOutboundControl({
    pool,
    tenantIds: ['10', '13'],
    logger,
    runControl,
  });

  assert.equal(first.tenants['10'].status, 'started');
  assert.equal(first.tenants['13'].status, 'started');
  assert.equal(second.tenants['10'].status, 'skipped');
  assert.equal(second.tenants['10'].reason, 'overlap');
  assert.equal(second.tenants['10'].cycleId, null);
  assert.equal(second.tenants['13'].status, 'skipped');
  assert.equal(second.tenants['13'].reason, 'overlap');
  assert.deepEqual(calls.sort(), ['10', '13']);

  release();
  await whenControlDispatchSettled();

  const third = await dispatchGovernedOutboundControl({
    pool,
    tenantIds: ['10'],
    logger,
    runControl: async ({ tenantId }) => {
      calls.push(`again:${tenantId}`);
      return { plan: { state: 'healthy' } };
    },
  });
  assert.equal(third.tenants['10'].status, 'started');
  assert.equal(third.tenants['13'], undefined);
  await whenControlDispatchSettled();
  assert.deepEqual(calls.filter((row) => row === 'again:10'), ['again:10']);
});

test('failure while admitting or running one tenant does not block the other', async () => {
  const calls = [];
  const errors = [];
  const pool = {
    async connect() {
      return {
        async query(sql, args) {
          if (args[1] === 10 && sql.includes('pg_try_advisory_lock')) {
            throw Object.assign(new Error('lock failed'), { code: 'lock_unavailable' });
          }
          if (sql.includes('pg_try_advisory_lock')) return { rows: [{ locked: true }] };
          if (sql.includes('pg_advisory_unlock')) return { rows: [] };
          throw new Error(`unexpected sql: ${sql}`);
        },
        release() {},
      };
    },
  };

  const admitted = await dispatchGovernedOutboundControl({
    pool,
    tenantIds: ['10', '13'],
    logger: { log() {}, error(...parts) { errors.push(parts.join(' ')); } },
    runControl: async ({ tenantId }) => {
      calls.push(tenantId);
      if (tenantId === '13') throw Object.assign(new Error('control failed'), { code: 'control_failed' });
      return { plan: { state: 'healthy' } };
    },
  });

  assert.equal(admitted.tenants['10'].status, 'skipped');
  assert.equal(admitted.tenants['10'].reason, 'lock_unavailable');
  assert.equal(admitted.tenants['13'].status, 'started');
  assert.deepEqual(calls, ['13']);
  await whenControlDispatchSettled();
  assert.ok(errors.some((line) => line.includes('control_failed') || line.includes('tenant cycle failed')));

  let release;
  const gate = new Promise((resolve) => { release = resolve; });
  const healthyPool = memoryPool();
  const isolated = [];
  const result = await dispatchGovernedOutboundControl({
    pool: healthyPool,
    tenantIds: ['10', '13'],
    logger: { log() {}, error(...parts) { errors.push(parts.join(' ')); } },
    runControl: async ({ tenantId }) => {
      isolated.push(`start:${tenantId}`);
      if (tenantId === '10') {
        await gate;
        throw Object.assign(new Error('scout failed'), { code: 'scout_failed' });
      }
      isolated.push('done:13');
      return { plan: { state: 'healthy' } };
    },
  });
  assert.equal(result.tenants['10'].status, 'started');
  assert.equal(result.tenants['13'].status, 'started');
  assert.ok(isolated.includes('done:13'));
  assert.equal(isolated.includes('done:10'), false);
  release();
  await whenControlDispatchSettled();
  const retry = await dispatchGovernedOutboundControl({
    pool: healthyPool,
    tenantIds: ['10'],
    logger: { log() {}, error() {} },
    runControl: async () => {
      isolated.push('retried:10');
      return { halted: 'no_enabled_program' };
    },
  });
  assert.equal(retry.tenants['10'].status, 'started');
  await whenControlDispatchSettled();
  assert.ok(isolated.includes('retried:10'));
});

test('single-tenant dispatch stays on that tenant and execute=false is forwarded', async () => {
  const seen = [];
  const pool = memoryPool();
  const result = await dispatchGovernedOutboundControl({
    pool,
    tenantId: '13',
    execute: false,
    logger: { log() {}, error() {} },
    runControl: async (options) => {
      seen.push(options);
      return { halted: 'no_enabled_program' };
    },
  });
  assert.deepEqual(Object.keys(result.tenants), ['13']);
  assert.equal(result.execute, false);
  assert.equal(result.tenants['13'].status, 'started');
  assert.equal(result.tenants['13'].tenant, '13');
  assert.equal(seen.length, 1);
  assert.equal(seen[0].tenantId, '13');
  assert.equal(seen[0].execute, false);
  assert.equal(seen[0].cycleId, result.tenants['13'].cycleId);
  await whenControlDispatchSettled();
});

test('cron control route returns compact per-tenant dispatch status', async () => {
  const dispatchPath = require.resolve('../services/governedOutboundControlDispatch');
  const previous = require.cache[dispatchPath];
  const calls = [];
  require.cache[dispatchPath] = {
    id: dispatchPath,
    filename: dispatchPath,
    loaded: true,
    exports: {
      dispatchGovernedOutboundControl: async (options) => {
        calls.push(options);
        const ids = options.tenantId ? [String(options.tenantId)] : ['10', '13'];
        const tenants = {};
        for (const tenant of ids) {
          tenants[tenant] = {
            tenant,
            status: 'started',
            cycleId: `moc_${tenant}_route`,
            reason: null,
            timestamp: '2026-09-29T00:00:00.000Z',
          };
        }
        return {
          bounded: true,
          dispatchedAt: '2026-09-29T00:00:00.000Z',
          execute: options.execute !== false,
          tenants,
        };
      },
    },
  };

  const previousSecret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = 'bounded-control-secret';
  delete require.cache[require.resolve('../routes/cron')];
  const router = require('../routes/cron');
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = await listen(app);

  try {
    const denied = await fetch(`${server.base}/cron/anchor-max-outbound-control`, { method: 'POST' });
    assert.equal(denied.status, 401);

    const all = await fetch(`${server.base}/cron/anchor-max-outbound-control?execute=false`, {
      method: 'POST',
      headers: { authorization: 'Bearer bounded-control-secret' },
    });
    const allBody = await all.json();
    assert.equal(all.status, 200);
    assert.equal(allBody.bounded, true);
    assert.equal(allBody.execute, false);
    assert.equal(allBody.tenants['10'].status, 'started');
    assert.equal(allBody.tenants['13'].status, 'started');
    assert.equal(JSON.stringify(allBody).includes('scout'), false);
    assert.equal(calls[0].execute, false);
    assert.equal(calls[0].tenantId, null);

    const one = await fetch(`${server.base}/cron/anchor-max-outbound-control?tenant_id=13&execute=false`, {
      method: 'POST',
      headers: { authorization: 'Bearer bounded-control-secret' },
    });
    const oneBody = await one.json();
    assert.equal(one.status, 200);
    assert.deepEqual(Object.keys(oneBody.tenants), ['13']);
    assert.equal(calls[1].tenantId, '13');

    require.cache[dispatchPath].exports.dispatchGovernedOutboundControl = async () => {
      const error = new Error('unsupported');
      error.code = 'unsupported_governed_outbound_tenant';
      throw error;
    };
    const unknown = await fetch(`${server.base}/cron/anchor-max-outbound-control?tenant_id=14`, {
      method: 'POST',
      headers: { authorization: 'Bearer bounded-control-secret' },
    });
    const unknownBody = await unknown.json();
    assert.equal(unknown.status, 500);
    assert.equal(unknownBody.error, 'unsupported_governed_outbound_tenant');
  } finally {
    await server.close();
    if (previous) require.cache[dispatchPath] = previous;
    else delete require.cache[dispatchPath];
    delete require.cache[require.resolve('../routes/cron')];
    if (previousSecret === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
  }
});
});

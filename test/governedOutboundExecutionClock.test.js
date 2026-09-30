'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const express = require('express');
const http = require('node:http');

const {
  executionClockEnabled,
  resolveTickTenantIds,
  runGovernedOutboundTickCycle,
  startGovernedOutboundExecutionClock,
} = require('../services/governedOutboundExecutionClock');

test('execution clock arms for mailbox tenants when Babrun sending is enabled', () => {
  const savedClock = process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED;
  const savedAnchorSched = process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED;
  const savedBabrun = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  const savedIds = process.env.GOVERNED_OUTBOUND_TENANT_IDS;
  try {
    delete process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED;
    delete process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED;
    delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    process.env.GOVERNED_OUTBOUND_TENANT_IDS = '10,13';
    assert.equal(executionClockEnabled(), false);
    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
    assert.equal(executionClockEnabled(), true);
    process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED = 'false';
    assert.equal(executionClockEnabled(), false);
  } finally {
    if (savedClock === undefined) delete process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED;
    else process.env.GOVERNED_OUTBOUND_EXECUTION_CLOCK_ENABLED = savedClock;
    if (savedAnchorSched === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_SCHEDULER_ENABLED = savedAnchorSched;
    if (savedBabrun === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = savedBabrun;
    if (savedIds === undefined) delete process.env.GOVERNED_OUTBOUND_TENANT_IDS;
    else process.env.GOVERNED_OUTBOUND_TENANT_IDS = savedIds;
  }
});

test('resolveTickTenantIds evaluates every enabled configured tenant', () => {
  const savedAnchor = process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  const savedBabrun = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  const savedIds = process.env.GOVERNED_OUTBOUND_TENANT_IDS;
  try {
    process.env.GOVERNED_OUTBOUND_TENANT_IDS = '10,13';
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
    assert.deepEqual(resolveTickTenantIds(), ['10', '13']);
    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'false';
    assert.deepEqual(resolveTickTenantIds(), ['10', '13']);
    assert.deepEqual(resolveTickTenantIds({ enabledTenantsOnly: true }), ['10']);
  } finally {
    if (savedAnchor === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = savedAnchor;
    if (savedBabrun === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = savedBabrun;
    if (savedIds === undefined) delete process.env.GOVERNED_OUTBOUND_TENANT_IDS;
    else process.env.GOVERNED_OUTBOUND_TENANT_IDS = savedIds;
  }
});

test('runGovernedOutboundTickCycle evaluates both tenants without cross-tenant coupling', async () => {
  const ticks = [];
  const mockCron = {
    run: async ({ tenantIds }) => {
      const results = {};
      for (const tenantId of tenantIds) {
        ticks.push(tenantId);
        if (tenantId === '10-block') throw new Error('tenant_10_failed');
        results[tenantId] = { tenantId, sent: 0, halted: tenantId === '13' ? 'spacing' : null };
      }
      return { tenants: results };
    },
  };
  const savedIds = process.env.GOVERNED_OUTBOUND_TENANT_IDS;
  const savedAnchor = process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
  const savedBabrun = process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
  try {
    process.env.GOVERNED_OUTBOUND_TENANT_IDS = '10,13';
    process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = 'true';
    process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = 'true';
    const result = await runGovernedOutboundTickCycle({
      pool: {},
      cron: mockCron,
      allTenants: true,
    });
    assert.deepEqual(ticks, ['10', '13']);
    assert.equal(result.tenants['13'].halted, 'spacing');
  } finally {
    if (savedIds === undefined) delete process.env.GOVERNED_OUTBOUND_TENANT_IDS;
    else process.env.GOVERNED_OUTBOUND_TENANT_IDS = savedIds;
    if (savedAnchor === undefined) delete process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED;
    else process.env.ANCHOR_GOVERNED_OUTBOUND_ENABLED = savedAnchor;
    if (savedBabrun === undefined) delete process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED;
    else process.env.BABRUN_GOVERNED_OUTBOUND_ENABLED = savedBabrun;
  }
});

test('in-process clock invokes tick for configured tenants on the five-minute cadence', async () => {
  let tickRuns = 0;
  const fakeCron = {
    poll: async () => ({ results: [] }),
    run: async () => {
      tickRuns += 1;
      return { tenants: { 13: { sent: 0 } } };
    },
  };
  const timers = [];
  const handle = startGovernedOutboundExecutionClock({
    enabled: true,
    autostart: false,
    pool: {},
    cron: fakeCron,
    maxControlEnabled: false,
    setInterval(fn) {
      timers.push(fn);
      return { unref() {} };
    },
    clearInterval() {},
    dispatchControl: async () => ({ tenants: {} }),
  });
  assert.ok(handle);
  await handle.cycle();
  assert.equal(tickRuns, 1);
  for (let i = 0; i < 4; i += 1) await handle.cycle();
  assert.equal(tickRuns, 1);
  await handle.cycle();
  assert.equal(tickRuns, 2);
});

test('cron governed-outbound-tick route mirrors anchor-daily-outbound and rejects bad secrets', async () => {
  const clockPath = require.resolve('../services/governedOutboundExecutionClock');
  const previousClock = require.cache[clockPath];
  require.cache[clockPath] = {
    id: clockPath,
    filename: clockPath,
    loaded: true,
    exports: {
      runGovernedOutboundTickCycle: async () => ({ tenants: { 10: { sent: 0 }, 13: { sent: 0 } } }),
    },
  };
  delete require.cache[require.resolve('../routes/cron')];
  const router = require('../routes/cron');
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = await new Promise((resolve) => {
    const s = http.createServer(app);
    s.listen(0, () => resolve({ server: s, base: `http://127.0.0.1:${s.address().port}` }));
  });
  const previousSecret = process.env.CRON_SECRET;
  process.env.CRON_SECRET = 'tick-secret';
  try {
    for (const path of ['/cron/governed-outbound-tick', '/cron/anchor-daily-outbound']) {
      const denied = await fetch(`${server.base}${path}`, { method: 'POST' });
      assert.equal(denied.status, 401);
      const ok = await fetch(`${server.base}${path}`, {
        method: 'POST',
        headers: { authorization: 'Bearer tick-secret' },
      });
      const body = await ok.json();
      assert.equal(ok.status, 200);
      assert.ok(body.tenants['10']);
      assert.ok(body.tenants['13']);
    }
  } finally {
    await new Promise((resolve, reject) => server.server.close((err) => (err ? reject(err) : resolve())));
    if (previousClock) require.cache[clockPath] = previousClock;
    else delete require.cache[clockPath];
    delete require.cache[require.resolve('../routes/cron')];
    if (previousSecret === undefined) delete process.env.CRON_SECRET;
    else process.env.CRON_SECRET = previousSecret;
  }
});

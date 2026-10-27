'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');
const express = require('express');

// Load the actual route, tenant normalizer, and service with fixture-only
// dependencies. No database connection, provider, or real scheduler is loaded.
function loadFixtureModule(relativePath, dependencies, globals = {}) {
  const filename = path.join(__dirname, '..', relativePath);
  const module = { exports: {} };
  vm.compileFunction(fs.readFileSync(filename, 'utf8'), ['module', 'require', 'process', 'console'], {
    filename,
  })(module, name => {
      if (Object.hasOwn(dependencies, name)) return dependencies[name];
      throw new Error(`Unexpected live dependency in cron test: ${name}`);
  }, globals.process, globals.console);
  return module.exports;
}

async function withCronFixture(options, callback) {
  const calls = { init: [], cycles: [], allClients: 0, logs: [], readiness: [] };
  const db = { query: async sql => {
    calls.readiness.push(sql);
    assert.match(sql, /^SELECT /);
    if (options.readinessError) throw options.readinessError;
    return { rows: sql.includes("WHERE id = 'global'") ? [{ id: 'global' }] : [] };
  } };
  const clientContext = loadFixtureModule('utils/clientContext.js', {
    '../db': db,
    './canonicalVerticals': { ensureCanonicalVerticalConstraint() {} },
  });
  const makeStore = name => class {
    constructor(_db, { clientId }) { this.clientId = clientId; }
    async init() {
      calls.init.push({ store: name, clientId: this.clientId });
      throw new Error('Scheduler must never initialize stores');
    }
  };
  const service = loadFixtureModule('services/maxAttentionService.js', {
    '../db': db,
    '../packages/max/attention': {
      PostgresAttentionStore: makeStore('attention'),
      async runAttentionCycle(context) {
        calls.cycles.push(context);
        if (options.cycleError) throw options.cycleError;
        return options.result;
      },
    },
    '../packages/max/stateIngestion': { PostgresStateStore: makeStore('state') },
    '../packages/max/decisionExecution': { PostgresDecisionStore: makeStore('decision') },
  });
  service.runAttentionSchedulerAllClients = async () => {
    calls.allClients += 1;
    throw new Error('All-client dispatch is forbidden');
  };
  const router = loadFixtureModule('routes/cron.js', {
    express,
    '../utils/meteredAnthropic': { forAgent: () => class {} },
    '../db': db,
    '../utils/clientContext': clientContext,
    '../utils/agentDispatchPolicy': {},
    '../scoutExpansion': {},
    '../utils/maxDecayCron': { createMaxDecayCronHandler: () => (_req, res) => res.sendStatus(404) },
    '../services/scoutPlacesDiagnostic': {},
    '../utils/anchorResearchProviderPause': require('../utils/anchorResearchProviderPause'),
    '../utils/researchProviderBlockLog': require('../utils/researchProviderBlockLog'),
    '../services/maxAttentionService': service,
  }, {
    process: { env: { CRON_SECRET: Object.hasOwn(options, 'secret') ? options.secret : 'route-test-secret' } },
    console: { error: (...args) => calls.logs.push(args) },
  });
  const app = express();
  app.use(express.json());
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await new Promise((resolve, reject) => {
    server.once('listening', resolve);
    server.once('error', reject);
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  async function request(query = '', body, { method = 'POST', secret = 'route-test-secret' } = {}) {
    const params = new URLSearchParams(query);
    if (secret != null) params.set('secret', secret);
    const response = await fetch(`${base}/cron/max-attention-cycle?${params}`, {
      method,
      ...(body ? { body: JSON.stringify(body), headers: { 'Content-Type': 'application/json' } } : {}),
    });
    return { status: response.status, cache: response.headers.get('cache-control'), body: await response.json() };
  }
  try {
    await callback({ calls, request });
  } finally {
    await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
  }
}

const successfulResult = {
  run_id: 'att_run_1234567890abcdef1234', claimed: 2, evaluated: 2, failed: 0,
  telemetry: { duplicate_evaluations: 1, wake_only: 0 },
};
const failureMessage = 'postgres://user:secret@database:5432/private?token=private-token';

test('POST successful cycle preserves result shape, explicit tenant, and time', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    const response = await request('client_id=11&now=2026-10-14T00:00:00Z');
    assert.equal(response.status, 200);
    assert.equal(response.cache, 'no-store');
    assert.deepEqual(response.body, { ...successfulResult, client_id: 11, ok: true });
    assert.equal(calls.cycles.length, 1);
    assert.equal(calls.cycles[0].clientId, 11);
    assert.equal(calls.cycles[0].now.toISOString(), '2026-10-14T00:00:00.000Z');
    assert.equal(calls.init.length, 0);
    assert.ok(calls.readiness.length > 0);
    assert.equal(calls.allClients, 0);
  });
});

test('GET returned item failure is HTTP 500 and ok:false, with sanitized diagnostics', async () => {
  const result = {
    ...successfulResult, evaluated: 1, failed: 1,
    telemetry: {
      duplicate_evaluations: 0, wake_only: 0,
      error_att_0123456789abcdef01234567: failureMessage,
      release_error_att_0123456789abcdef01234567: failureMessage,
      claim_lost_att_0123456789abcdef01234567: true,
    },
  };
  await withCronFixture({ result }, async ({ calls, request }) => {
    const response = await request('clientId=10', undefined, { method: 'GET' });
    assert.equal(response.status, 500);
    assert.equal(response.cache, 'no-store');
    assert.equal(response.body.ok, false);
    assert.equal(response.body.client_id, 10);
    assert.equal(response.body.run_id, result.run_id);
    assert.equal(response.body.failed, 1);
    assert.equal(response.body.claimed, 2);
    assert.equal(response.body.evaluated, 1);
    assert.equal(response.body.telemetry.error_att_0123456789abcdef01234567, 'attention_item_failed');
    assert.equal(response.body.telemetry.release_error_att_0123456789abcdef01234567, 'attention_claim_release_failed');
    assert.equal(response.body.telemetry.claim_lost_att_0123456789abcdef01234567, true);
    assert.doesNotMatch(JSON.stringify(response.body), /secret|private-token|postgres:\/\//);
    assert.equal(result.telemetry.error_att_0123456789abcdef01234567, failureMessage);
    assert.equal(calls.cycles[0].clientId, 10);
    assert.equal(calls.allClients, 0);
  });
});

for (const [name, options, expectedCycleCalls] of [
  ['schema readiness', { readinessError: new Error(failureMessage) }, 0],
  ['scheduler', { cycleError: Object.assign(new Error(failureMessage), { code: failureMessage }) }, 1],
]) {
  test(`${name} exception stays HTTP 500 without exposing exception text`, async () => {
    await withCronFixture(options, async ({ calls, request }) => {
      const response = await request('client_id=11');
      assert.equal(response.status, 500);
      assert.equal(response.cache, 'no-store');
      assert.deepEqual(response.body, { ok: false, error: 'max_attention_cycle_failed' });
      assert.equal(calls.cycles.length, expectedCycleCalls);
      assert.equal(calls.allClients, 0);
      assert.doesNotMatch(JSON.stringify([response.body, calls.logs]), /secret|private-token|postgres:\/\//);
    });
  });
}

test('wrong or absent cron secret denies GET and POST dispatch before initialization', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    for (const method of ['GET', 'POST']) {
      for (const secret of ['wrong-secret', null, '']) {
        const response = await request('client_id=11', undefined, { method, secret });
        assert.equal(response.status, 401);
        assert.deepEqual(response.body, { error: 'Unauthorized' });
      }
    }
    assert.equal(calls.init.length, 0);
    assert.equal(calls.cycles.length, 0);
    assert.equal(calls.allClients, 0);
  });
});

for (const configuredSecret of [undefined, '']) {
  test(`missing cron configuration (${configuredSecret === undefined ? 'unset' : 'empty'}) denies dispatch`, async () => {
    await withCronFixture({ secret: configuredSecret, result: successfulResult }, async ({ calls, request }) => {
      for (const method of ['GET', 'POST']) {
        for (const secret of [null, '', 'route-test-secret']) {
          const response = await request('client_id=11', undefined, { method, secret });
          assert.equal(response.status, 401);
          assert.deepEqual(response.body, { error: 'Unauthorized' });
        }
      }
      const bodyResponse = await request('client_id=11', { secret: 'route-test-secret' }, { secret: null });
      assert.equal(bodyResponse.status, 401);
      assert.deepEqual(bodyResponse.body, { error: 'Unauthorized' });
      assert.equal(calls.init.length, 0);
      assert.equal(calls.cycles.length, 0);
      assert.equal(calls.allClients, 0);
    });
  });
}

test('body secret and body time keep the existing authentication/time behavior', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    const response = await request('client_id=11', {
      secret: 'route-test-secret', now: '2026-10-14T01:00:00Z',
    }, { secret: null });
    assert.equal(response.status, 200);
    assert.equal(calls.cycles[0].clientId, 11);
    assert.equal(calls.cycles[0].now.toISOString(), '2026-10-14T01:00:00.000Z');
    assert.equal(calls.allClients, 0);
  });
});

test('missing, malformed, repeated and conflicting tenant selectors fail before store initialization', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    for (const [query, body] of [
      ['', undefined], ['client_id=', undefined], ['client_id=invalid', undefined],
      ['client_id=0', undefined], ['client_id=-3', undefined], ['client_id=11junk', undefined],
      ['client_id=1.5', undefined], ['client_id=01', undefined], ['client_id=9007199254740992', undefined],
      ['client_id=11&client_id=12', undefined], ['client_id=11&clientId=10', undefined],
      ['client_id=11', { client_id: 10 }], ['', { client_id: null }],
      ['', { client_id: true }], ['', { client_id: [11] }], ['', { client_id: {} }],
      ['client_id=11', { clientId: '' }],
    ]) {
      const response = await request(query, body);
      assert.equal(response.status, 400, JSON.stringify([query, body]));
      assert.equal(response.body.error, 'max_attention_explicit_tenant_required');
    }
    assert.equal(calls.init.length, 0);
    assert.equal(calls.cycles.length, 0);
    assert.equal(calls.allClients, 0);
  });
});

test('body tenant and consistent aliases dispatch exactly one explicit tenant', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    for (const [query, body] of [['', { client_id: 11 }], ['', { clientId: '11' }],
      ['client_id=11&clientId=11', { client_id: 11, clientId: '11' }]]) {
      const response = await request(query, body);
      assert.equal(response.status, 200);
      assert.equal(response.body.client_id, 11);
    }
    assert.deepEqual(calls.cycles.map(call => call.clientId), [11, 11, 11]);
    assert.equal(calls.allClients, 0);
  });
});

test('an empty due-work cycle remains successful for an explicit tenant', async () => {
  const result = { ...successfulResult, claimed: 0, evaluated: 0, telemetry: { duplicate_evaluations: 0, wake_only: 0 } };
  await withCronFixture({ result }, async ({ calls, request }) => {
    const response = await request('client_id=11');
    assert.equal(response.status, 200);
    assert.deepEqual(response.body, { ...result, client_id: 11, ok: true });
    assert.equal(calls.cycles.length, 1);
    assert.equal(calls.allClients, 0);
  });
});


test('cron limit accepts only consistent integer 1..20, preserving default 20', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    for (const [query, body, expected] of [
      ['client_id=11', undefined, 20], ['client_id=11&limit=1', undefined, 1],
      ['client_id=11', { limit: 2 }, 2], ['client_id=11&limit=20', { limit: 20 }, 20],
    ]) {
      assert.equal((await request(query, body)).status, 200);
      assert.equal(calls.cycles.at(-1).limit, expected);
    }
  });
});

test('malformed and conflicting limits reject before readiness or scheduler dispatch', async () => {
  await withCronFixture({ result: successfulResult }, async ({ calls, request }) => {
    for (const value of [null, '', 0, -1, 21, 1.5, true, [], {}, '01', '1x', '9007199254740992']) {
      const response = await request('client_id=11', { limit: value });
      assert.equal(response.status, 400, JSON.stringify(value));
      assert.equal(response.body.error, 'max_attention_invalid_limit');
    }
    assert.equal((await request('client_id=11&limit=1&limit=2')).status, 400);
    assert.equal((await request('client_id=11&limit=1', { limit: 2 })).status, 400);
    assert.equal(calls.cycles.length, 0);
    assert.equal(calls.readiness.length, 0);
    assert.equal(calls.init.length, 0);
  });
});

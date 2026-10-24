'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const { runClientsStartup, PHASES } = require('../utils/clientsStartup');
function loadContext(query) {
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(require.resolve('../utils/clientContext'), 'utf8'), {
    module, exports: module.exports,
    require(name) {
      if (name === '../db') return { query };
      if (name === './canonicalVerticals') return { ensureCanonicalVerticalConstraint: async () => {} };
      throw new Error('Unexpected import');
    },
  });
  return module.exports.ensureClientArchitecture;
}
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
test('concurrent callers share one execution; later calls recheck', async () => {
  const gate = deferred(); let starts = 0;
  const ensure = loadContext(async sql => {
    if (/CREATE TABLE IF NOT EXISTS clients/.test(sql)) { starts++; await gate.promise; }
    return { rows: [] };
  });
  const calls = Array.from({ length: 16 }, () => ensure());
  assert.ok(calls.every(p => p === calls[0]));
  await Promise.resolve(); assert.equal(starts, 1);
  gate.resolve(); await Promise.all(calls);
  await ensure(); assert.equal(starts, 2);
});
test('failure clears shared state without automatic retry', async () => {
  const gate = deferred(); let starts = 0;
  const failure = Object.assign(new Error('private database detail'), { code: '40P01' });
  const ensure = loadContext(async sql => {
    if (/CREATE TABLE IF NOT EXISTS clients/.test(sql) && ++starts === 1) await gate.promise;
    return { rows: [] };
  });
  const results = Promise.allSettled([ensure(), ensure()]); gate.reject(failure);
  assert.ok((await results).every(r => r.status === 'rejected' && r.reason === failure));
  assert.equal(starts, 1); await ensure(); assert.equal(starts, 2);
});
function recorder() {
  const events = [];
  return { events, logger: { log: (...args) => events.push(args), error: (...args) => events.push(args) } };
}
test('records ordered phase success and completion', async () => {
  const { events, logger } = recorder(); const calls = [];
  const steps = Object.fromEntries(PHASES.map(phase => [phase, async () => calls.push(phase)]));
  assert.equal(await runClientsStartup(steps, logger), true);
  assert.deepEqual(calls, [...PHASES]);
  assert.deepEqual(events.map(e => e[1]), [...PHASES, 'complete'].map(phase => ({ phase, outcome: 'succeeded' })));
});
for (const code of ['40P01', '40001', 'secret-credential', undefined]) {
  test(`failure stops later phases and sanitizes logs (${code ?? 'no code'})`, async () => {
    const { events, logger } = recorder(); const calls = [];
    const error = Object.assign(new Error('postgres://user:secret@host SQL parameter=private'), {
      code, detail: 'private detail', query: 'private SQL', parameters: ['private value'],
    });
    const steps = Object.fromEntries(PHASES.map(phase => [phase, async () => {
      calls.push(phase); if (phase === PHASES[2]) throw error;
    }]));
    assert.equal(await runClientsStartup(steps, logger), false);
    assert.deepEqual(calls, PHASES.slice(0, 3));
    assert.deepEqual(events.at(-1)[1], { phase: PHASES[2], outcome: 'failed', sqlstate: ['40P01', '40001'].includes(code) ? code : 'unknown' });
    assert.doesNotMatch(JSON.stringify(events), /private|secret|postgres|parameters|detail/);
  });
}

test('actual server startup uses the named chain without starting live dependencies', async () => {
  const { events, logger } = recorder();
  const noop = () => Promise.resolve();
  const stub = new Proxy(noop, { get: () => noop });
  let completion;
  let middlewareCount = 0;
  const startup = fs.readFileSync(require.resolve('../server'), 'utf8').split('app.use(session(')[0];
  vm.runInNewContext(startup, {
    process: { env: {}, on() {} }, console: logger,
    require(name) {
      if (name === 'express') return () => ({ use(fn) { assert.equal(typeof fn, 'function'); middlewareCount++; } });
      if (name === './utils/anchorPortalReadiness') return require('../utils/anchorPortalReadiness');
      if (name === './utils/clientsStartup') return {
        runClientsStartup(steps) {
          assert.deepEqual(Object.keys(steps), [...PHASES]);
          completion = runClientsStartup(steps, logger);
          return completion;
        },
      };
      if (name === './services/governedOutboundTenant') return { parseGovernedOutboundTenantIds: () => [] };
      return stub;
    },
  });
  assert.equal(middlewareCount, 1);
  assert.ok(completion);
  assert.equal(await completion, true);
  assert.equal(events.length, PHASES.length + 1);
});

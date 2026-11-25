'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const soda = require('../services/leviScoutSodaClient');

function timeoutError() {
  return Object.assign(new Error('The operation was aborted due to timeout'), {
    name: 'TimeoutError',
    code: 23,
  });
}

test('TimeoutError code 23 is transient and does not escape readJson', async () => {
  assert.equal(soda.isTransientFetchError(timeoutError()), true);
  const telemetry = [];
  const result = await soda.readJson({
    url: 'https://data.cityofchicago.org/resource/ezma-pppn.json',
    fetchImpl: async () => { throw timeoutError(); },
    wait: async () => {},
    telemetry,
  });
  assert.deepEqual(result.rows, []);
  assert.equal(result.status, 'timeout');
  assert.equal(result.attempts, soda.MAX_ATTEMPTS);
  assert.equal(telemetry.filter(event => event.status === 'timeout').length, soda.MAX_ATTEMPTS);
});

test('transient failures retry with backoff and can recover', async () => {
  let attempts = 0;
  const waits = [];
  const result = await soda.readJson({
    url: 'https://data.cityofchicago.org/resource/r5kz-chrr.json',
    fetchImpl: async () => {
      attempts += 1;
      if (attempts < 3) throw timeoutError();
      return { ok: true, json: async () => [{ legal_name: 'Ben Operator LLC' }] };
    },
    wait: async (ms) => { waits.push(ms); },
  });
  assert.equal(attempts, 3);
  assert.deepEqual(waits, [250, 750]);
  assert.equal(result.status, 'ok');
  assert.equal(result.rows.length, 1);
});

test('lookup budget skips further SODA requests and paid URLs stay forbidden', async () => {
  const budget = soda.createLookupBudget(1);
  const first = await soda.boundedReadJson({
    fetchImpl: async () => ({ ok: true, json: async () => [] }),
    url: 'https://data.cityofchicago.org/resource/ezma-pppn.json',
    budget,
  });
  const second = await soda.boundedReadJson({
    fetchImpl: async () => { throw new Error('unbounded'); },
    url: 'https://data.cityofchicago.org/resource/r5kz-chrr.json',
    budget,
  });
  assert.equal(first.status, 'empty');
  assert.equal(second.status, 'skipped_budget');
  assert.equal(budget.used(), 1);
  await assert.rejects(soda.readJson({
    url: 'https://hunter.io/v2/email-finder',
    fetchImpl: async () => ({ ok: true, json: async () => [] }),
  }), { code: 'levi_research_paid_provider_approval_required' });
});

test('four-request budget counts HTTP retries across both Chicago datasets and never issues a fifth request', async () => {
  let calls = 0;
  const budget = soda.createLookupBudget(4);
  const fetchImpl = async () => {
    calls += 1;
    throw timeoutError();
  };
  const owners = await soda.boundedReadJson({
    fetchImpl,
    url: 'https://data.cityofchicago.org/resource/ezma-pppn.json',
    budget,
    wait: async () => {},
  });
  const licenses = await soda.boundedReadJson({
    fetchImpl,
    url: 'https://data.cityofchicago.org/resource/r5kz-chrr.json',
    budget,
    wait: async () => {},
  });
  const extra = await soda.boundedReadJson({
    fetchImpl,
    url: 'https://data.cityofchicago.org/resource/ezma-pppn.json',
    budget,
    wait: async () => {},
  });
  assert.equal(calls, 4);
  assert.equal(budget.used(), 4);
  assert.equal(owners.status, 'timeout');
  assert.equal(licenses.status, 'timeout');
  assert.equal(extra.status, 'skipped_budget');
});

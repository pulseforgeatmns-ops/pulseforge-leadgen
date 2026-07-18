'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { FLAG_NAMES, assertRevenueFlag, loadRevenueFlags } = require('../utils/revenueFlags');

test('revenue flags stay off unless both environment and tenant settings enable them', async () => {
  const db = { query: async () => ({ rows: [Object.fromEntries(FLAG_NAMES.map(name => [name, true]))] }) };
  const disabled = await loadRevenueFlags(db, 10, {});
  assert.deepEqual(disabled, Object.fromEntries(FLAG_NAMES.map(name => [name, false])));
  const partial = await loadRevenueFlags(db, 10, { REVENUE_SCHEMA_ENABLED: 'true', REVENUE_OPERATOR_READS_ENABLED: 'true' });
  assert.equal(partial.revenue_schema_enabled, true);
  assert.equal(partial.revenue_operator_reads_enabled, true);
  assert.equal(partial.revenue_operator_writes_enabled, false);
});

test('missing feature flag schema fails closed', async () => {
  const db = { query: async () => { throw Object.assign(new Error('missing'), { code: '42P01' }); } };
  const flags = await loadRevenueFlags(db, 10, { REVENUE_SCHEMA_ENABLED: 'true' });
  assert.ok(FLAG_NAMES.every(name => flags[name] === false));
  assert.throws(() => assertRevenueFlag(flags, 'revenue_operator_reads_enabled'), { code: 'REVENUE_CAPABILITY_DISABLED' });
});

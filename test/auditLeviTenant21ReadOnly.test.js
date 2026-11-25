'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { chooseTenantColumn, inspect } = require('../scripts/auditLeviTenant21ReadOnly');

test('prefers client_id, falls back to tenant_id, never scans unscoped table', () => {
  assert.equal(chooseTenantColumn(['id', 'client_id', 'tenant_id']), 'client_id');
  assert.equal(chooseTenantColumn(['tenant_id']), 'tenant_id');
  assert.equal(chooseTenantColumn(['id']), undefined);
});
test('missing table produces no data query', async () => {
  let calls = 0;
  const result = await inspect({ query: async () => { calls++; return { rows: [] }; } }, 'prospects');
  assert.deepEqual(result, { table: 'prospects', available: false });
  assert.equal(calls, 1);
});
test('table without tenant scope is never scanned', async () => {
  let calls = 0;
  const result = await inspect({ query: async () => { calls++; return { rows: [{ column_name: 'id' }, { column_name: 'status' }] }; } }, 'prospects');
  assert.equal(result.tenantScoped, false);
  assert.equal(calls, 1);
});
test('all record queries are tenant scoped and do not return PII', async () => {
  const calls = [];
  const client = { query: async (sql, params) => {
    calls.push({ sql, params });
    if (sql.includes('information_schema')) return { rows: ['client_id', 'source', 'email', 'phone'].map(column_name => ({ column_name })) };
    if (sql.includes('AS total')) return { rows: [{ total: 29 }] };
    if (sql.includes('AS populated')) return { rows: [{ populated: 0 }] };
    return { rows: [{ value: 'scout_research_only', count: 10 }] };
  }};
  const result = await inspect(client, 'prospects');
  assert.equal(result.total, 29);
  assert.equal(result.groups.source[0].count, 10);
  assert.equal(result.completeness.email, 0);
  assert.equal(calls.length, 5);
  for (const c of calls.slice(1)) {
    assert.match(c.sql, /"client_id"::text = \$1/);
    assert.deepEqual(c.params, ['21']);
  }
  assert.equal(JSON.stringify(result).includes('operator@example'), false);
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { parse } = require('../scripts/rebuildRevenueProjections');

test('revenue rebuild CLI supports tenant, all-tenant, range, compare, dry-run, and apply modes', () => {
  assert.deepEqual(parse(['--client-id=10','--dry-run']), { apply: false, compareOnly: false, clientId: 10 });
  assert.deepEqual(parse(['--all-tenants','--compare-only']), { apply: false, compareOnly: true, allTenants: true });
  const apply = parse(['--client-id=10','--apply','--from=2026-07-01T00:00:00Z','--to=2026-08-01T00:00:00Z']);
  assert.equal(apply.apply, true);
  assert.equal(apply.from, '2026-07-01T00:00:00.000Z');
  assert.throws(() => parse(['--apply']), /Choose --client-id/);
  assert.throws(() => parse(['--client-id=10','--record']), /requires --compare-only/);
  assert.throws(() => parse(['--client-id=10','--all-tenants']), /mutually exclusive/);
});

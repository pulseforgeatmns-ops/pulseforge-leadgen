'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const {
  scoreProspectForAo,
  segmentAffinityForAo,
  assertMinimumAoAccountCounts,
  DEFAULT_MIN_ACCOUNTS_PER_AO,
} = require('../utils/aoAccountFill');

test('Rory segment affinity favors law and accounting over large PM accounts', () => {
  const affinity = segmentAffinityForAo('Rory');
  assert.ok(affinity.includes('law_firm'));
  assert.ok(affinity.includes('accounting'));

  const lawScore = scoreProspectForAo({
    prospect: { vertical: 'law_firm', icp_score: 90 },
    company: { location: 'Manchester NH' },
    aoName: 'Rory',
  });
  const bigPmScore = scoreProspectForAo({
    prospect: { vertical: 'property_manager', icp_score: 91 },
    company: { location: 'Manchester NH' },
    aoName: 'Rory',
  });
  assert.ok(lawScore > bigPmScore, 'Rory should rank law/accounting-style accounts above large PM');
});

test('assertMinimumAoAccountCounts fails when an AO is under minimum', async () => {
  const db = {
    query: async (sql, params) => {
      const normalized = sql.replace(/\s+/g, ' ').trim();
      if (/FROM users[\s\S]*role = 'ao'/i.test(normalized)) {
        return {
          rows: [
            { id: 25, name: 'Rory', email: 'r@x.com', active: true },
            { id: 20, name: 'Mike', email: 'm@x.com', active: true },
          ],
        };
      }
      if (/COUNT\(\*\)/i.test(normalized) && params[1] === 25) {
        return { rows: [{ n: 1 }] };
      }
      if (/COUNT\(\*\)/i.test(normalized) && params[1] === 20) {
        return { rows: [{ n: 12 }] };
      }
      return { rows: [] };
    },
  };

  const result = await assertMinimumAoAccountCounts({
    clientId: 10,
    db,
    minPerAo: DEFAULT_MIN_ACCOUNTS_PER_AO,
  });
  assert.equal(result.ok, false);
  assert.equal(result.failures.length, 1);
  assert.equal(result.failures[0].ao_name, 'Rory');
});

test('assertMinimumAoAccountCounts passes when all AOs meet minimum', async () => {
  const db = {
    query: async (sql) => {
      const normalized = sql.replace(/\s+/g, ' ').trim();
      if (/FROM users[\s\S]*role = 'ao'/i.test(normalized)) {
        return { rows: [{ id: 25, name: 'Rory', active: true }] };
      }
      if (/COUNT\(\*\)/i.test(normalized)) {
        return { rows: [{ n: 15 }] };
      }
      return { rows: [] };
    },
  };

  const result = await assertMinimumAoAccountCounts({ clientId: 10, db, minPerAo: 10 });
  assert.equal(result.ok, true);
});

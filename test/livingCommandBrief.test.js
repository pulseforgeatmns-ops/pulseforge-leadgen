'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildLivingCommandBrief, resolveLivingGoogleAds } = require('../services/livingCommandBrief');
const { buildGoogleAdsOperatorBrief } = require('../packages/penny-paid-acquisition/googleAdsOperatorBrief');

test('opening brief scopes every fact query to the selected workspace', async () => {
  const queries = [];
  const pool = { query: async (sql, params) => {
    queries.push({ sql, params });
    return { rows: [sql.includes('FROM prospects') ? { total: 17, new_24h: 2, warm: 3, booked: 1 } : { pending: 4 }] };
  } };
  const now = new Date('2026-10-19T12:00:00Z');
  const brief = await buildLivingCommandBrief({ clientId: 10, pool, now, googleAds: { live: { available: false } } });
  assert.equal(queries.length, 2);
  queries.forEach(query => { assert.deepEqual(query.params, [10]); assert.match(query.sql, /client_id = \$1/); });
  assert.deepEqual(brief.pipeline, { total: 17, new24h: 2, warm: 3, booked: 1, windowLabel: 'Last 24 hours' });
  assert.deepEqual(brief.approvals, { pending: 4 });
  assert.equal(brief.generatedAt, now.toISOString());
});

test('partial failure stays unavailable while other facts survive', async () => {
  const pool = { query: async sql => {
    if (sql.includes('FROM prospects')) throw new Error('offline');
    return { rows: [{ pending: 0 }] };
  } };
  const brief = await buildLivingCommandBrief({ clientId: 11, pool });
  assert.equal(brief.pipeline, null);
  assert.deepEqual(brief.approvals, { pending: 0 });
  assert.equal(brief.googleAds, null);
});

test('missing or invalid workspace cannot query the database', async () => {
  for (const clientId of [null, 0, -1, 'other', 1.5]) {
    await assert.rejects(buildLivingCommandBrief({ clientId, pool: { query() { assert.fail('must not query'); } } }), /workspace/);
  }
});

test('Penny evidence survives a failed field brief and stays scoped to the selected tenant', async () => {
  const pool = {};
  const evidence = { live: { available: true, campaigns: [{ name: 'Search', status: 'ENABLED' }] } };
  const section = await resolveLivingGoogleAds({
    clientId: 10, pool, operatorBrief: null,
    loadGoogleAds: async input => {
      assert.deepEqual(input, { tenantId: 10, pool });
      return evidence;
    },
  });
  assert.equal(section, evidence);
  assert.equal(await resolveLivingGoogleAds({ clientId: 10, pool, operatorBrief: { googleAds: evidence }, loadGoogleAds() { assert.fail('should reuse account evidence'); } }), evidence);
  assert.deepEqual(await resolveLivingGoogleAds({ clientId: 11, pool, loadGoogleAds: async () => { throw new Error('unavailable'); } }), { live: { available: false } });
});

test('Ads opening facts retain campaign status, dates and unknown conversions', () => {
  const brief = buildGoogleAdsOperatorBrief({
    readiness: { blockers: [], warnings: [], accountStatus: 'READY' },
    evidence: {
      account: { currency: 'GBP' }, observationWindow: { start: '2026-10-13', end: '2026-10-19' },
      campaigns: [
        { name: 'Search', status: 'ENABLED', spend: 20, clicks: 3, platformConversions: null, budget: { amount: 25, period: 'DAILY' } },
        { name: 'PMax', status: 'PAUSED', spend: 100, clicks: 40, platformConversions: 1 },
      ],
    },
  });
  assert.equal(brief.live.currency, 'GBP');
  assert.equal(brief.live.campaigns[0].conversions, null);
  assert.equal(brief.live.campaigns[0].dailyBudget, 25);
  assert.equal(brief.live.campaigns[1].status, 'PAUSED');
  assert.equal(brief.live.window.end, '2026-10-19');
  assert.deepEqual(buildGoogleAdsOperatorBrief({ readiness: { blockers: ['No connection'] } }).live, { available: false });
});

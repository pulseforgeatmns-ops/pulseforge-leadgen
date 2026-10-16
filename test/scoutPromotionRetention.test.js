'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
for (const [path, exports] of [
  ['../db', { query: async () => { throw new Error('unexpected default database access'); } }],
  ['../utils/scoutUnenrichedSchema', { ensureScoutUnenrichedTable: async () => {} }],
  ['../utils/emailVerificationSchema', { ensureEmailVerificationColumns: async () => {} }],
]) {
  const id = require.resolve(path);
  require.cache[id] = { id, filename: id, loaded: true, exports };
}
const { run } = require('../scoutUnenrichedEnrichmentAgent');
const { defaultScoutRamp } = require('../services/maxOutboundControlLoop');
const { promotionCohort } = require('../services/scoutPromotionCohort');

test('actual enrichment worker and ramp retain identity without serializing it', async () => {
  const logs = [];
  const db = { query: async (sql, params) => {
    if (/SELECT \*/.test(sql)) return { rows: [{ id: 1, client_id: 13 }] };
    if (/INSERT INTO agent_log/.test(sql)) logs.push(params[1]);
    else assert.match(sql, /UPDATE scout_unenriched/);
    return { rows: [] };
  } };
  const originalLog = console.log;
  console.log = (...args) => logs.push(JSON.stringify(args));
  try {
    const ramp = await defaultScoutRamp({
      pool: db, store: { tenantId: '13', clientId: 13 },
      program: { id: 'fixture', tenant_id: '13' },
      source: { id: 'fixture', payload: { structuredMission: {
        market: { segment: 'small_business_owners' }, geography: { region: 'United States', scope: 'nationwide' },
      } } }, plan: { deficit: 1 }, skipVerificationRetry: true,
      enrichment: { run: params => run({ ...params, promote: async () => ({
        promoted: true, recovered: false, prospectId: '765432109', emailResolved: true, emailVerified: true,
      }) }) },
      recoverExistingInventory: async () => ({ payload: { qualifiedCount: 0 } }),
      runDiscovery: async () => { throw new Error('unexpected provider request'); },
    });
    const cohort = promotionCohort(ramp.enrichmentBatches, {
      clean: [], excluded: [{ prospectId: '765432109', reason: 'contact_classification_not_authorized' }],
    });
    assert.equal(ramp.promoted, 1);
    assert.equal(cohort.identifiedProspects, 1);
    assert.equal(cohort.exclusionCounts.contact_classification_not_authorized, 1);
    assert.ok(!JSON.stringify(ramp).includes('765432109'));
    assert.ok(!JSON.stringify(logs).includes('765432109'));
  } finally { console.log = originalLog; }
});

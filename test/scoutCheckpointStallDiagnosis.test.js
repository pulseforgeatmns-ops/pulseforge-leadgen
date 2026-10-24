'use strict';
// Diagnostic-only synthetic fixtures. No live DB, provider, or website access.
const test = require('node:test');
const assert = require('node:assert/strict');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: {
  query: async () => { throw new Error('Unexpected shared database call'); },
} };
const { _test: { persistDiscoveredCompanies } } = require('../services/maxOutboundControlLoop');
const { createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { constructCandidateUniverse } = require('../packages/max/scoutAcquisition/CandidateUniverse');

function candidate(i) {
  return { id: `p${i}`, placeId: `p${i}`, name: `Fixture ${i}`,
    website: `https://fixture${i}.example`, address: 'Manchester, NH', location: 'Manchester, NH',
    industry: 'Property Management Manchester NH', discoveryQuery: 'Property Management Manchester NH',
    discoveryConcept: 'Property Management' };
}
function persistenceContext() {
  const inserts = [];
  return { inserts,
    pool: { query: async (sql, p) => {
      if (/INSERT INTO scout_unenriched/.test(sql)) { inserts.push(p); return { rowCount: 1, rows: [{ id: 'fake' }] }; }
      assert.match(sql, /FROM prospects p/); return { rows: [], rowCount: 0 };
    } },
    store: { tenantId: '10', clientId: 10, candidateOwnership: async () => null },
    options: { tenantId: '10', scoutContext: { scope: { eligibleSubsegments: ['property_manager'] },
      allowedCities: ['manchester'], investigateBusinessEvidence: true } },
  };
}
test('diagnosis: restarting replays the same 20 unresolved sites and starves a classifiable twenty-first', async t => {
  const websitePath = require.resolve('../services/scoutWebsiteBusinessEvidence');
  const original = require.cache[websitePath];
  const investigated = [];
  require.cache[websitePath] = { id: websitePath, filename: websitePath, loaded: true,
    exports: { acquireBusinessEvidence: async company => {
      investigated.push(company.id);
      return company.id === 'p20'
        ? { candidate: { ...company, description: 'We provide property management services' }, evidence: { source: 'synthetic' } }
        : null;
    } } };
  t.after(() => { if (original) require.cache[websitePath] = original; else delete require.cache[websitePath]; });
  const { pool, store, options, inserts } = persistenceContext();
  const rows = Array.from({ length: 21 }, (_, i) => candidate(i));
  for (let run = 0; run < 3; run++) {
    const receipt = await persistDiscoveredCompanies(pool, store, { ...options, companies: structuredClone(rows) });
    assert.equal(receipt.checkpointComplete, false);
    assert.equal(receipt.admission.websiteInvestigations, 20);
    assert.equal(receipt.inserted, 0);
  }
  assert.equal(investigated.length, 60);
  assert.equal(new Set(investigated).size, 20);
  assert.ok(!investigated.includes('p20'));
  // The same valid candidate succeeds when it is finally offered the cap slot.
  const finalReceipt = await persistDiscoveredCompanies(pool, store, { ...options, companies: [rows[20]] });
  assert.equal(finalReceipt.inserted, 1);
  assert.equal(finalReceipt.checkpointComplete, true);
  assert.equal(inserts.length, 1);
});

test('diagnosis: one unresolved candidate keeps an independently completed empty query replayable', async () => {
  const rows = new Map();
  const store = { tenantId: '10',
    load: async key => rows.get(key) || null,
    stage: async (key, candidates) => { rows.set(key, { state: 'fetched', candidates }); return rows.get(key); },
    acknowledge: async keys => keys.forEach(key => rows.set(key, { state: 'handled', candidates: [] })),
  };
  const checkpoint = createCheckpointSession({ store });
  const empty = await checkpoint.lookup('Empty Query Manchester NH', false);
  const blocked = await checkpoint.lookup('Blocked Query Manchester NH', false);
  await checkpoint.stage(empty.key, []);
  await checkpoint.stage(blocked.key, [candidate(1)]);
  const context = persistenceContext();
  const universe = await constructCandidateUniverse({
    searchDefinition: { tenantId: '10', geography: { label: 'Manchester NH', state: 'NH', cities: ['Manchester'] },
      segments: ['property_management'], businessNeed: 'commercial_cleaning' },
    forceDiscover: true, existing: { companies: [], people: [] },
    adapters: [{ id: 'public_business_places', sourceType: 'public_business_data', available: () => true,
      discover: async () => ({ candidates: [candidate(1)], errors: [] }) }],
    persistCompanies: async input => persistDiscoveredCompanies(context.pool, context.store,
      { ...input, ...context.options, scoutContext: { ...context.options.scoutContext, investigateBusinessEvidence: false } }),
    acknowledgeDiscovery: () => checkpoint.acknowledge(),
  });
  assert.deepEqual(universe.discoveryErrors, []);
  assert.equal(checkpoint.snapshot().pendingAcknowledgements, 2);
  assert.equal(rows.get(empty.key).state, 'fetched');
  assert.equal(rows.get(blocked.key).state, 'fetched');
});

test('cursor design model: persisted ring cursor reaches all 41 sites within three bounded passes without acknowledging unresolved work', async t => {
  const websitePath = require.resolve('../services/scoutWebsiteBusinessEvidence');
  const original = require.cache[websitePath];
  let thisPass = [];
  require.cache[websitePath] = { id: websitePath, filename: websitePath, loaded: true,
    exports: { acquireBusinessEvidence: async company => {
      thisPass.push(company.id);
      return company.id === 'p040'
        ? { candidate: { ...company, description: 'We provide property management services' }, evidence: { source: 'synthetic' } }
        : null;
    } } };
  t.after(() => { if (original) require.cache[websitePath] = original; else delete require.cache[websitePath]; });
  const sourceRows = Array.from({ length: 41 }, (_, i) => ({ ...candidate(i), id: `p${String(i).padStart(3, '0')}` }));
  // Distinct source rows can resolve to one normalized scheduling identity.
  // Keep the raw duplicate; it must not consume another investigation slot.
  sourceRows.push({ ...candidate(0), id: 'alias-p000', website: 'https://WWW.FIXTURE0.EXAMPLE/about' });
  const candidateKey = row => new URL(row.website).hostname.toLowerCase().replace(/^www\./, '');
  const sourceBefore = structuredClone(sourceRows);
  // Retain the original design model separately from the new runtime cursor.
  // A test-only in-memory seam instruments the non-cursor branch without changing
  // the on-disk source or candidate loop order.
  const fs = require('node:fs');
  const path = require('node:path');
  const Module = require('node:module');
  const filename = require.resolve('../services/maxOutboundControlLoop');
  const source = fs.readFileSync(filename, 'utf8');
  const branch = "&& websiteInvestigations < 20\n";
  assert.equal(source.split(branch).length, 2, 'review this model if the production branch changes');
  const modeledSource = source.replace(branch,
    '&& websiteInvestigations < 20 && scoutContext.shouldInvestigateBusinessEvidence(company)\n');
  const modeledModule = new Module(filename, module);
  modeledModule.filename = filename;
  modeledModule.paths = Module._nodeModulePaths(path.dirname(filename));
  modeledModule._compile(modeledSource, filename);
  const modeledPersist = modeledModule.exports._test.persistDiscoveredCompanies;
  // Model only: this fixture does not exercise the production cursor store.
  // The append-only receipt survives each reconstructed pass; fetched payload does not change.
  const receipts = [];
  const allVisited = new Set();
  const { pool, store, options } = persistenceContext();
  for (let run = 0; run < 3; run++) {
    thisPass = [];
    const cursor = receipts.at(-1)?.afterKey || null;
    const sorted = [...new Set(sourceRows.map(candidateKey))].sort();
    const after = cursor ? sorted.findIndex(key => key > cursor) : 0;
    const start = after < 0 ? 0 : after;
    const selected = [...sorted.slice(start), ...sorted.slice(0, start)].slice(0, 20);
    const selectedKeys = new Set(selected);
    const consumedKeys = new Set();
    const receipt = await modeledPersist(pool, store, { ...options, companies: structuredClone(sourceRows),
      scoutContext: { ...options.scoutContext, shouldInvestigateBusinessEvidence: row => {
        const key = candidateKey(row);
        if (!selectedKeys.has(key) || consumedKeys.has(key)) return false;
        consumedKeys.add(key);
        return true;
      } } });
    assert.equal(thisPass.length, 20);
    assert.equal(consumedKeys.size, 20, 'each actual call consumes a distinct normalized identity');
    assert.ok(!thisPass.includes('alias-p000'), 'duplicate raw rows cannot consume a second slot');
    assert.deepEqual(thisPass, sourceRows.filter(row => thisPass.includes(row.id)).map(row => row.id),
      'the original candidate loop order is unchanged');
    assert.equal(receipt.checkpointComplete, false, 'unresolved work remains replayable');
    for (const key of thisPass) allVisited.add(key);
    // With wraparound, the selected ring endpoint differs from the last visited
    // array item. Persist the former, never the last callback or an array index.
    receipts.push({ afterKey: selected.at(-1) });
  }
  assert.equal(allVisited.size, 41);
  assert.ok(allVisited.has('p040'));
  assert.deepEqual(sourceRows, sourceBefore, 'immutable replay payload remains unchanged');
  assert.equal(receipts.length, 3, 'one progress receipt per completed pass');
});

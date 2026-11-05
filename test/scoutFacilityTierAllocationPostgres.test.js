'use strict';
// SPEC-SCOUT-P1-001 Rev. 2 (#1048) on disposable native PostgreSQL: the
// migration, its permanence and rollback guards, and crash recovery through
// the REAL tenant memory, page store, checkpoint session, Places provider and
// planner SQL. Synthetic provider; no network.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { Pool } = require('pg'); const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: { query: async () => ({ rows: [] }) } };
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true,
  exports: { reserveApiCall: async () => 'synthetic', settleApiCall: async () => {} } };
const { createTenantDiscoveryMemory } = require('../services/scoutTenantDiscoveryMemory');
const { createCheckpointStore, createCheckpointSession } = require('../services/scoutQueryCheckpoints');
const { createPageCheckpointStore } = require('../services/scoutQueryContinuation');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const coverage = require('../services/scoutFacilityResearchCoverage');
const { ANCHOR_FACILITY_TIERS: CONFIG } = require('../config/anchorFacilityTiers');

const migration = file => fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8');
const scope = { eligibleSubsegments: [...CONFIG.scope.eligibleSubsegments], cities: [...CONFIG.scope.cities] };
const request = { tenantId: '10', scope: 'market', evidenceType: 'identity', segment: 'managed_facilities',
  geography: { cities: scope.cities, state: 'NH' } };

test('tier allocation migration and crash recovery on native PostgreSQL', { timeout: 180000 }, async t => {
  const pg = await startDisposablePostgres('scout-tier-allocation-'), db = new Pool({ connectionString: pg.connectionString, max: 8 });
  t.after(async () => { await db.end(); await pg.stop(); });
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
    '2026-10-19-scout-candidate-tickets.sql', '2026-10-27-scout-tenant-search-memory.sql', '2026-10-28-scout-research-only-grants.sql'])
    await db.query(migration(file));

  await t.test('before the migration the planner is inert and reads nothing it cannot', async () => {
    const s = coverage.createFacilityResearchCoverage({ pool: db, tenantId: '10', scope, tierAllocation: CONFIG });
    const v1 = coverage.createFacilityResearchCoverage({ pool: db, tenantId: '10', scope });
    const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
    assert.deepEqual(await s.plan(request, buildQueriesForEvidence), await v1.plan(request, buildQueriesForEvidence));
    assert.equal(s.snapshot().tierAllocation.status, 'not_activated');
  });

  await t.test('migration applies, rolls back while unused, and reapplies', async () => {
    await db.query(migration('2026-11-04-scout-tier-allocation.sql'));
    await db.query(migration('2026-11-04-scout-tier-allocation.rollback.sql'));
    assert.equal((await db.query("SELECT to_regclass('scout_query_allocation_activations') AS r")).rows[0].r, null);
    await db.query(migration('2026-11-04-scout-tier-allocation.sql'));
  });

  await t.test('activation and qualification records are constrained and permanent', async () => {
    await assert.rejects(db.query(`INSERT INTO scout_query_allocation_activations(tenant_id,allocation_version,approval_ref)
      VALUES('13',$1,'x')`, [CONFIG.version]), /check/);
    await assert.rejects(db.query(`INSERT INTO scout_query_allocation_activations(tenant_id,allocation_version,approval_ref)
      VALUES('10',$1,' ')`, [CONFIG.version]), /check/);
    await db.query(`INSERT INTO scout_query_allocation_activations(tenant_id,allocation_version,approval_ref)
      VALUES('10',$1,'synthetic-approval')`, [CONFIG.version]);
    await assert.rejects(db.query('UPDATE scout_query_allocation_activations SET approval_ref=$1', ['changed']), /permanent/);
    await assert.rejects(db.query('DELETE FROM scout_query_allocation_activations'), /permanent/);
    await assert.rejects(db.query(`INSERT INTO scout_candidate_commercial_qualifications
      (research_candidate_id,tenant_id,state,source,actor,rubric_version,evidence_refs) VALUES(1,'10','maybe','human_review','a','r','["e"]')`), /check/);
    await assert.rejects(db.query(`INSERT INTO scout_candidate_commercial_qualifications
      (research_candidate_id,tenant_id,state,source,actor,rubric_version,evidence_refs) VALUES(1,'10','qualified','human_review','a','r','[]')`), /check/);
    await assert.rejects(db.query(migration('2026-11-04-scout-tier-allocation.rollback.sql')), /in_use/);
  });

  await t.test('a crash after the paid call is never refetched or recharged and is counted once as uncertain', async () => {
    const searches = [];
    const cycle = async (fp, { crashBeforeStage = false } = {}) => {
      const tenantMemory = createTenantDiscoveryMemory({ pool: db, tenantId: '10', fingerprint: fp });
      const pages = createPageCheckpointStore({ db, tenantId: '10', fingerprint: fp });
      const pageStore = crashBeforeStage ? { ...pages, plan: pages.plan, reserve: pages.reserve, fail: pages.fail,
        stage: async () => { throw Object.assign(Error('synthetic_process_crash'), { code: 'synthetic_process_crash' }); } } : pages;
      const session = createCheckpointSession({ store: createCheckpointStore({ db, tenantId: '10', fingerprint: fp }), pageStore, tenantMemory });
      const planner = coverage.createFacilityResearchCoverage({ pool: db, tenantId: '10', scope, tierAllocation: CONFIG });
      const provider = createPlacesProvider({ apiKey: 'synthetic', useCache: false, queryCheckpointSession: session,
        tenantDiscoveryMemory: tenantMemory, facilityResearchCoverage: planner,
        fetchImpl: async url => { searches.push(new URL(url).searchParams.get('query'));
          return { ok: true, status: 200, json: async () => ({ status: 'ZERO_RESULTS', results: [] }) }; } });
      let error = null;
      try { await provider.collectEvidence(request); } catch (e) { error = e; }
      if (!error) await session.acknowledge();
      return { error, receipt: planner.snapshot().tierAllocation };
    };
    const first = await cycle('fp-a', { crashBeforeStage: true });
    assert.equal(first.error?.code, 'synthetic_process_crash');
    assert.equal(first.receipt.intendedTier, 1);
    assert.equal(searches.length, 1);
    const crashed = searches[0];
    assert.deepEqual((await db.query("SELECT state FROM scout_query_pages")).rows, [{ state: 'reserved' }]);

    const second = await cycle('fp-a');
    assert.equal(second.error, null);
    assert.equal(second.receipt.position, 2);
    assert.deepEqual(second.receipt.countsByStatus[1], { completed: 0, uncertain: 1 });
    const third = await cycle('fp-b'); // another mission fingerprint
    assert.equal(third.receipt.position, 3);
    assert.equal(searches.filter(q => q === crashed).length, 1, 'never repurchased by either mission');
    assert.equal(new Set(searches).size, searches.length);
    const owners = (await db.query("SELECT count(*)::int AS n FROM scout_tenant_query_owners")).rows[0].n;
    assert.equal(owners, 3, 'ownership claims kept; none removed');
    assert.deepEqual(third.receipt.counts, { 1: 1, 2: 1, 3: 0 }, 'receipt counts precede this cycle\'s own selection');
    assert.equal(second.receipt.intendedTier, 2); assert.equal(third.receipt.intendedTier, 1);
    assert.deepEqual(third.receipt.excludedPages, { pre_ledger: 0, other_version: 0, unknown_version: 0, unattributed: 0 });
  });
});

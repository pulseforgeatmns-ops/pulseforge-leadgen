'use strict';
// SPEC-SCOUT-P0-008 Part A (#1072): cross-mission discovery memory on real
// PostgreSQL. Search coverage (what was bought), business identity (which
// places are known) and mission eligibility (does it qualify now) stay separate:
// a later mission reuses a confirmed purchase without buying it again, and its
// own ticket ledger re-evaluates every business. Synthetic Places only.
require('./helpers/denyExternalProviders.cjs');
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const dbPath = require.resolve('../db');
require.cache[dbPath] = { id: dbPath, filename: dbPath, loaded: true, exports: { query: async () => ({ rows: [] }) } };
const spendPath = require.resolve('../utils/apiSpendControl');
require.cache[spendPath] = { id: spendPath, filename: spendPath, loaded: true,
  exports: { reserveApiCall: async () => 'synthetic', settleApiCall: async () => {} } };
const { createTenantDiscoveryMemory, isTenantDiscoveryMemoryEnabled } = require('../services/scoutTenantDiscoveryMemory');
const { createCheckpointStore, createCheckpointSession, queryIdentity, PLAN_VERSION } = require('../services/scoutQueryCheckpoints');
const { createPageCheckpointStore } = require('../services/scoutQueryContinuation');
const { createCandidateTicketStore } = require('../services/scoutCandidateTickets');
const { createPlacesProvider } = require('../packages/capabilities/discovery/providers/PlacesProvider');

const OPT_IN = { SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS: '13' };

test('SPEC-SCOUT-P0-008: a new mission reuses confirmed search evidence instead of buying it again', { timeout: 120000 }, async t => {
  const pg = await startDisposablePostgres('scout-cross-mission-'), db = new Pool({ connectionString: pg.connectionString, max: 8 });
  const previous = process.env.SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS;
  t.after(async () => { if (previous == null) delete process.env.SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS;
    else process.env.SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS = previous; await db.end(); await pg.stop(); });
  for (const file of ['2026-10-17-scout-query-checkpoints.sql', '2026-10-19-scout-query-pages.sql',
    '2026-10-19-scout-candidate-tickets.sql', '2026-10-27-scout-tenant-search-memory.sql'])
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations', file), 'utf8'));

  const calls = [];
  const request = (tenantId, concept, city = 'Bedford') => ({ tenantId, scope: 'market', segment: 'small_business_owners',
    evidenceType: 'identity', discoveryConcept: concept, geography: { cities: [city], state: 'NH' } });
  // One mission run = one restarted worker: fresh stores, memory and provider.
  function mission(tenantId, fingerprint, ids, env = OPT_IN) {
    const tenantMemory = createTenantDiscoveryMemory({ pool: db, tenantId, fingerprint, env });
    const tickets = createCandidateTicketStore({ pool: db, tenantId, fingerprint });
    const session = createCheckpointSession({ store: createCheckpointStore({ db, tenantId, fingerprint }),
      ticketStore: tickets, pageStore: createPageCheckpointStore({ db, tenantId, fingerprint }), tenantMemory });
    const provider = createPlacesProvider({ apiKey: 'synthetic', useCache: false, queryCheckpointSession: session,
      tenantDiscoveryMemory: tenantMemory, fetchImpl: async input => {
        const u = new URL(input), detail = u.pathname.includes('details'), id = u.searchParams.get('place_id');
        calls.push({ tenantId, fingerprint, kind: detail ? 'detail' : 'search', id, query: u.searchParams.get('query') });
        return { ok: true, status: 200, json: async () => detail
          ? { status: 'OK', result: { place_id: id, name: `Fixture cleaning ${id}`, website: `https://${id}.example`,
            formatted_address: 'Bedford NH', types: ['point_of_interest'] } }
          : { status: 'OK', results: ids.map(place_id => ({ place_id, name: `Fixture ${place_id}` })) } };
      } });
    return { tenantMemory, tickets, session,
      run: async (concept, city) => { await tenantMemory.assertReady(); return provider.collectEvidence(request(tenantId, concept, city)); } };
  }
  const ticketCount = async (tenantId, fingerprint) => (await db.query(`SELECT count(*)::int n FROM scout_candidate_work
    WHERE tenant_id=$1 AND search_fingerprint=$2`, [tenantId, fingerprint])).rows[0].n;

  await t.test('other tenants fail closed until explicitly opted in AND migrated', async () => {
    assert.equal(isTenantDiscoveryMemoryEnabled('13', {}), false);
    assert.throws(() => createTenantDiscoveryMemory({ pool: db, tenantId: '13', fingerprint: 'fp', env: {} }), /binding_required/);
    assert.throws(() => createTenantDiscoveryMemory({ pool: db, tenantId: '1', fingerprint: 'fp', env: { SCOUT_TENANT_DISCOVERY_MEMORY_TENANTS: '1' } }),
      /binding_required/, 'a tenant off the durable ticket path cannot opt in');
    await assert.rejects(createTenantDiscoveryMemory({ pool: db, tenantId: '13', fingerprint: 'fp', env: OPT_IN }).assertReady(),
      /tenant_schema_required/, 'the original Tenant-10-only schema blocks before any provider work');
    await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-11-06-scout-tenant-search-memory-all-tenants.sql'), 'utf8'));
    await createTenantDiscoveryMemory({ pool: db, tenantId: '13', fingerprint: 'fp', env: OPT_IN }).assertReady();
  });

  await t.test('1-4: mission B reuses mission A\'s purchase with zero provider calls and its own tickets', async () => {
    const a = mission('13', 'mission-a', ['alpha', 'beta']);
    const first = await a.run('Residential Cleaning');
    assert.equal(first.length, 2);
    assert.deepEqual(calls.map(c => c.kind), ['search', 'detail', 'detail']);
    const before = calls.length;
    const b = mission('13', 'mission-b', ['alpha', 'beta']);
    const reused = await b.run('Residential Cleaning');
    assert.equal(calls.length, before, 'no search and no detail request for an equivalent search');
    assert.deepEqual(reused.map(r => r.placeId).sort(), ['alpha', 'beta']);
    assert.ok(reused.every(r => r.searchEvidenceReuse?.sourceSearchFingerprint === 'mission-a'), 'provenance names the source mission');
    assert.equal(b.session.snapshot().reusedQueries, 1);
    // Mission eligibility is separate: B holds its own pending obligations for
    // these businesses, independent of how mission A resolved them.
    assert.equal(await ticketCount('13', 'mission-b'), 2);
    assert.equal((await db.query(`SELECT count(*)::int n FROM scout_candidate_work WHERE tenant_id='13'
      AND search_fingerprint='mission-b' AND state='pending'`)).rows[0].n, 2);
  });

  await t.test('7: restart and redeploy keep coverage; reused evidence is handled once', async () => {
    const before = calls.length;
    const again = mission('13', 'mission-b', ['alpha', 'beta']);
    assert.deepEqual(await again.run('Residential Cleaning'), [], 'already handled under mission B');
    const third = mission('13', 'mission-c', ['alpha', 'beta']);
    assert.equal((await third.run('Residential Cleaning')).length, 2, 'a third mission also reuses it');
    assert.equal(calls.length, before);
    assert.equal(await ticketCount('13', 'mission-b'), 2, 'no duplicate obligations after restart');
  });

  await t.test('5: a genuinely different search (other town) is still bought', async () => {
    const before = calls.length;
    const b = mission('13', 'mission-b', ['gamma']);
    const result = await b.run('Residential Cleaning', 'Hooksett');
    assert.equal(result.length, 1);
    assert.deepEqual(calls.slice(before).map(c => c.kind), ['search', 'detail']);
    assert.match(calls[before].query, /Hooksett/);
  });

  await t.test('6: another tenant never sees this tenant\'s memory', async () => {
    const before = calls.length;
    const anchor = mission('10', 'anchor-mission', ['alpha', 'beta'], {});
    const result = await anchor.run('Residential Cleaning');
    assert.equal(result.length, 2);
    assert.deepEqual(calls.slice(before).map(c => c.kind), ['search', 'detail', 'detail'], 'tenant 10 buys its own copy');
    const key = queryIdentity('Residential Cleaning Bedford NH', false);
    assert.equal(await createTenantDiscoveryMemory({ pool: db, tenantId: '10', fingerprint: 'other' })
      .priorEvidence(key, 'Residential Cleaning Bedford NH').then(e => e.candidates.every(c => c.searchEvidenceReuse.sourceSearchFingerprint === 'anchor-mission')), true);
  });

  await t.test('9: uncertain responses are never reused or bought; partial evidence is flagged, not complete', async () => {
    // An in-flight / unknown page purchase by mission A, no confirmed response.
    const uncertainKey = queryIdentity('Office Cleaning Bedford NH', false);
    await db.query(`INSERT INTO scout_query_pages(tenant_id,search_fingerprint,query_key,root_query_key,page_index,state,reservation_id)
      VALUES('13','mission-a',$1,$1,0,'reserved',gen_random_uuid())`, [uncertainKey]);
    await db.query(`INSERT INTO scout_tenant_query_owners(tenant_id,query_key,root_query_key,query_text,search_fingerprint)
      VALUES('13',$1,$1,'Office Cleaning Bedford NH','mission-a')`, [uncertainKey]);
    const before = calls.length;
    const b = mission('13', 'mission-b', ['delta']);
    assert.deepEqual(await b.run('Office Cleaning'), []);
    assert.equal(calls.length, before, 'no repurchase of an uncertain purchase');
    assert.equal(b.session.snapshot().continuation.statuses.tenant_query_outcome_unconfirmed, 1);
    assert.equal(b.session.snapshot().reusedQueries, 0);

    // Page 0 confirmed, page 1 failed: page 0 is reused, flagged as partial.
    const partialKey = queryIdentity('Carpet Cleaning Bedford NH', false);
    const page1 = require('../services/scoutQueryContinuation').pageIdentity(partialKey, 1);
    const raw = { placeId: 'epsilon', source: 'google_places', companyName: 'Epsilon Carpet', website: 'https://epsilon.example', address: 'Bedford NH' };
    await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates,handled_at)
      VALUES('13','mission-a',$1,$2,'handled',$3::jsonb,now())`, [partialKey, PLAN_VERSION, JSON.stringify([raw])]);
    await db.query(`INSERT INTO scout_query_pages(tenant_id,search_fingerprint,query_key,root_query_key,page_index,state,reservation_id,fetched_at,next_page_token)
      VALUES('13','mission-a',$1,$1,0,'fetched',gen_random_uuid(),now(),'synthetic-token')`, [partialKey]);
    await db.query(`INSERT INTO scout_query_pages(tenant_id,search_fingerprint,query_key,root_query_key,page_index,state,reservation_id,request_token,blocked_reason)
      VALUES('13','mission-a',$1,$2,1,'blocked',gen_random_uuid(),'synthetic-token','page_provider_failure')`, [page1, partialKey]);
    const c = mission('13', 'mission-b', ['epsilon']);
    const reused = await c.run('Carpet Cleaning');
    assert.deepEqual(reused.map(r => r.placeId), ['epsilon']);
    assert.equal(calls.length, before, 'the failed page is not bought by another mission');
    assert.equal(c.session.snapshot().continuation.statuses.tenant_query_evidence_partial, 1);
    assert.equal(c.tenantMemory.snapshot().searchEvidencePartial, 1);
  });

  await t.test('8: the current mission\'s own coverage is never treated as foreign evidence', async () => {
    // Same-mission continuation and coverage-epoch refresh stay with the page
    // store: tenant memory only consults OTHER missions of this tenant.
    const key = queryIdentity('Window Cleaning Bedford NH', true);
    await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates,handled_at)
      VALUES('13','solo-mission',$1,$2,'handled','[]'::jsonb,now())`, [key, PLAN_VERSION]);
    const memory = createTenantDiscoveryMemory({ pool: db, tenantId: '13', fingerprint: 'solo-mission', env: OPT_IN });
    assert.equal(await memory.checkQuery(key, 'Window Cleaning Bedford NH'), null);
    assert.equal(await memory.priorEvidence(key, 'Window Cleaning Bedford NH'), null);
  });

  await t.test('evidence from another tenant inside a payload is rejected, not reused', async () => {
    const key = queryIdentity('Pressure Washing Bedford NH', true);
    await db.query(`INSERT INTO scout_query_checkpoints(tenant_id,search_fingerprint,query_key,plan_version,state,candidates,handled_at)
      VALUES('13','mission-a',$1,$2,'handled',$3::jsonb,now())`, [key, PLAN_VERSION, JSON.stringify([{ placeId: 'zeta', tenantId: '10' }])]);
    await assert.rejects(createTenantDiscoveryMemory({ pool: db, tenantId: '13', fingerprint: 'mission-b', env: OPT_IN })
      .priorEvidence(key, 'Pressure Washing Bedford NH'), /evidence_tenant_mismatch/);
  });
});

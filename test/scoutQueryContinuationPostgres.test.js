'use strict';
// Disposable local PostgreSQL only. External connection strings are never read.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { execFileSync } = require('node:child_process');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createCheckpointStore, createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
const { createPageCheckpointStore, pageIdentity } = require('../services/scoutQueryContinuation');
const available = ['initdb','pg_ctl'].every(bin => { try { execFileSync('sh',['-c',`command -v ${bin}`],{stdio:'pipe'}); return true; } catch { return false; } });
const required = process.env.SCOUT_CHECKPOINT_REQUIRE_POSTGRES === 'true';
test('page continuation durability, atomic staging, crash recovery and concurrent reservation on real PostgreSQL', {
  skip: !available && !required ? 'Local PostgreSQL unavailable; required release gate must not skip' : false,
}, async t => {
  assert.ok(available, 'Required local PostgreSQL binaries are missing');
  const pg = await startDisposablePostgres('scout-pages-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 3 });
  t.after(() => closePostgresFixture(db, pg));
  for (const file of ['2026-10-17-scout-query-checkpoints.sql','2026-10-19-scout-query-pages.sql']) {
    await db.query(fs.readFileSync(path.join(__dirname,'../migrations',file),'utf8'));
  }
  const tenantId = '13'; const fingerprint = randomUUID(); let now = Date.now();
  const pageStore = createPageCheckpointStore({ db, tenantId, fingerprint, now: () => now });
  const store = createCheckpointStore({ db, tenantId, fingerprint });
  const key = queryIdentity('Painting Contractor United States', false);
  const candidates = [{ companyName: 'Local Painter', placeId: 'fixture-id', website: 'painter.example' }];
  const meta = { nextPageToken: 'opaque-page-two', rawResults: [{ place_id: 'fixture-id', name: 'Local Painter', types: ['painter'] }] };
  await pageStore.assertReady();
  const first = await pageStore.plan(key);
  const races = await Promise.all([pageStore.reserve(first), pageStore.reserve(first)]);
  assert.equal(races.filter(Boolean).length, 1);
  const reserved = { ...first, reservationId: races.find(Boolean) };
  assert.equal((await pageStore.plan(key)).status, 'page_fetch_in_progress');
  await pageStore.stage(reserved, candidates, meta);
  assert.deepEqual((await pageStore.plan(key)).row.candidates, candidates);
  const raw = (await db.query('SELECT * FROM scout_query_pages WHERE tenant_id=$1 AND search_fingerprint=$2',[tenantId,fingerprint])).rows[0];
  assert.equal(raw.next_page_token, 'opaque-page-two'); assert.equal(raw.raw_results.length,1);
  assert.equal(raw.state,'fetched');
  await assert.rejects(pageStore.stage(reserved, [{ companyName:'overwrite' }], meta), /reservation lost/);
  assert.deepEqual((await store.load(key)).candidates, candidates);
  assert.equal((await createPageCheckpointStore({db,tenantId:'10',fingerprint}).plan(key)).index,0);
  assert.equal((await createPageCheckpointStore({db,tenantId,fingerprint:randomUUID()}).plan(key)).index,0);
  // A crafted continuation cannot bypass durable handling or token binding.
  assert.equal(await pageStore.reserve({rootKey:key,key:pageIdentity(key,1),index:1,pageToken:'opaque-page-two'}),null);
  await store.acknowledge([key]);
  assert.equal((await pageStore.plan(key)).status,'page_token_not_ready');
  now += 6000;
  const second = await pageStore.plan(key);
  assert.equal(second.index,1); assert.equal(second.pageToken,'opaque-page-two');
  assert.equal(await pageStore.reserve({...second,pageToken:'wrong-token'}),null);
  const secondId = await pageStore.reserve(second);
  await pageStore.stage({...second,reservationId:secondId},candidates,{...meta,nextPageToken:null});
  await store.acknowledge([second.key]);
  assert.equal((await pageStore.plan(key)).status,'finite_query_exhausted');
  const terminal = createCheckpointSession({store,pageStore});
  await terminal.lookup('Painting Contractor United States',false);
  assert.ok(!JSON.stringify(terminal.snapshot()).includes('opaque-page-two'));

  const legacyKey = queryIdentity('Legacy Query United States',false);
  await store.stage(legacyKey,candidates); await store.acknowledge([legacyKey]);
  assert.equal((await pageStore.plan(legacyKey)).status,'legacy_continuation_unknown');

  // Prove checkpoint and token publication is one transaction.
  const crashKey = queryIdentity('Crash Query United States',false);
  const crashPlan = await pageStore.plan(crashKey);
  const crashId = await pageStore.reserve(crashPlan);
  const failingDb = { query: db.query.bind(db), connect: async () => {
    const client = await db.connect(); return { release: client.release.bind(client), query: async (sql,p) => {
      if (/UPDATE scout_query_pages SET state='fetched'/.test(sql)) throw Error('injected precommit failure');
      return client.query(sql,p);
    } };
  } };
  await assert.rejects(createPageCheckpointStore({db:failingDb,tenantId,fingerprint}).stage(
    {...crashPlan,reservationId:crashId},candidates,meta), /precommit/);
  assert.equal(await store.load(crashKey),null);
  assert.equal((await pageStore.plan(crashKey)).status,'page_fetch_in_progress');
  now += 3600001;
  assert.equal((await pageStore.plan(crashKey)).status,'page_fetch_outcome_unknown');

  // An uncertain COMMIT may have succeeded: restarting must replay it, not fetch again.
  const unknownKey = queryIdentity('Unknown Commit United States',false);
  const unknown = await pageStore.plan(unknownKey); unknown.reservationId = await pageStore.reserve(unknown);
  const uncertainDb = { query: db.query.bind(db), connect: async () => {
    const client = await db.connect(); return { release: client.release.bind(client), query: async (sql,p) => {
      const result = await client.query(sql,p); if (sql==='COMMIT') throw Error('injected lost commit reply'); return result;
    } };
  } };
  await assert.rejects(createPageCheckpointStore({db:uncertainDb,tenantId,fingerprint}).stage(unknown,candidates,meta), /lost commit/);
  assert.deepEqual((await pageStore.plan(unknownKey)).row.candidates,candidates);

  const deniedKey = queryIdentity('Budget Denied United States',false);
  const denied = await pageStore.plan(deniedKey); denied.reservationId = await pageStore.reserve(denied);
  await pageStore.fail(denied,{noNetwork:true}); assert.equal((await pageStore.plan(deniedKey)).index,0);
  const blockedKey = queryIdentity('Invalid Token United States',false);
  const blocked = await pageStore.plan(blockedKey); blocked.reservationId = await pageStore.reserve(blocked);
  await pageStore.fail(blocked,{googleStatus:'INVALID_REQUEST'});
  assert.equal((await pageStore.plan(blockedKey)).status,'continuation_token_invalid');
});

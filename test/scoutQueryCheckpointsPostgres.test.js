'use strict';
// Only a disposable local cluster is used; DATABASE_URL/TEST_DATABASE_URL are never read.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');
const { randomUUID } = require('node:crypto');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createCheckpointStore, createCheckpointSession, queryIdentity } = require('../services/scoutQueryCheckpoints');
function available(name) {
  try { execFileSync('sh', ['-c', `command -v ${name}`], { stdio: 'pipe' }); return true; }
  catch {
    try { return fs.existsSync(path.join(execFileSync('pg_config', ['--bindir'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim(), name)); }
    catch { return false; }
  }
}
const availablePg = available('initdb') && available('pg_ctl');
const required = process.env.SCOUT_CHECKPOINT_REQUIRE_POSTGRES === 'true';
test('durable scoped immutable checkpoint staging and acknowledgement on disposable PostgreSQL', {
  skip: !availablePg && !required ? 'Local PostgreSQL binaries unavailable; required release CI must set SCOUT_CHECKPOINT_REQUIRE_POSTGRES=true' : false,
}, async t => {
  assert.ok(availablePg, 'Release validation requires local PostgreSQL binaries');
  const pg = await startDisposablePostgres('scout-query-checkpoint-');
  const db = createTrackedPostgresPool({ connectionString: pg.connectionString, max: 2 });
  t.after(() => closePostgresFixture(db, pg));
  await db.query(fs.readFileSync(path.join(__dirname, '../migrations/2026-10-17-scout-query-checkpoints.sql'), 'utf8'));
  const tenantId = randomUUID(); const otherTenant = randomUUID(); const fingerprint = randomUUID();
  const a = createCheckpointStore({ db, tenantId, fingerprint });
  const b = createCheckpointStore({ db, tenantId: otherTenant, fingerprint });
  const key = queryIdentity('Cleaning Company North Charleston SC', false);
  const hit = { placeId: randomUUID(), companyName: 'Fixture Cleaning', website: 'fixture.example', address: 'North Charleston SC' };
  assert.equal(await a.load(key), null);
  await a.stage(key, [hit]);
  await a.stage(key, [{ ...hit, companyName: 'Must not overwrite' }]);
  assert.deepEqual((await a.load(key)).candidates, [hit]);
  assert.equal(await b.load(key), null);
  const restart = createCheckpointSession({ store: a });
  assert.deepEqual((await restart.lookup('Cleaning Company North Charleston SC', false)).candidates, [hit]);
  await restart.acknowledge();
  assert.equal((await a.load(key)).state, 'handled'); assert.deepEqual((await a.load(key)).candidates, []);
  await a.acknowledge([key]); // Concurrent acknowledgement is idempotent.
  await assert.rejects(b.acknowledge([key]), /not confirmed/);
  await assert.rejects(a.acknowledge([queryIdentity('Unknown Query', false)]), /not confirmed/);
  const nextMission = createCheckpointStore({ db, tenantId, fingerprint: randomUUID() });
  assert.equal(await nextMission.load(key), null);
});

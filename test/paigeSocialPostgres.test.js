'use strict';

const { describe, it, before, after, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { Pool } = require('pg');
const { createPostgresSocialContentStore } = require('../packages/capabilities/contentGeneration/SocialContentStore');
const { fixture } = require('./helpers/paigeSocialFixture');

const disposables = [];
const inFlight = new Set();
const unexpectedPoolErrors = [];

let publisher = null;
let outcomeBridge = null;
let receiptRecovery = null;
let generationMirror = null;
let pgClient = null;
let pgPool = null;
let pgInstance = null;
let shuttingDown = false;

function isPostgresShutdownError(err) {
  const message = String((err && err.message) || '');
  return Boolean(err && (err.code === '57P01' || /terminating connection due to administrator command/i.test(message)));
}

function trackPromise(promise) {
  if (!promise || typeof promise.then !== 'function') return promise;
  inFlight.add(promise);
  promise.finally(() => inFlight.delete(promise));
  return promise;
}

function registerDisposable(fn) {
  disposables.push(fn);
}

async function stopHandle(handle) {
  if (!handle) return;
  if (typeof handle.stop === 'function') await handle.stop();
  else if (typeof handle.close === 'function') await handle.close();
}

function onPoolError(err) {
  if (shuttingDown && isPostgresShutdownError(err)) return;
  unexpectedPoolErrors.push(err);
}

test.afterEach(async () => {
  const cleanup = disposables.splice(0).reverse();
  await Promise.allSettled(cleanup.map(async (dispose) => {
    await dispose();
  }));
  if (inFlight.size > 0) {
    await Promise.allSettled([...inFlight]);
  }
});

test.after(async () => {
  shuttingDown = true;
  await stopHandle(publisher);
  await stopHandle(outcomeBridge);
  await stopHandle(receiptRecovery);
  await stopHandle(generationMirror);
  if (inFlight.size > 0) {
    await Promise.allSettled([...inFlight]);
  }
  if (pgClient?.release) pgClient.release();
  if (pgClient?.end) await pgClient.end();
  if (pgPool && !pgPool.ended && typeof pgPool.end === 'function') await pgPool.end();
  if (pgInstance?.stop) await pgInstance.stop();
  if (pgPool) pgPool.off('error', onPoolError);
  pgClient = null;
  pgPool = null;
  pgInstance = null;
  if (unexpectedPoolErrors.length) throw unexpectedPoolErrors[0];
});

test('PostgreSQL locks, receipt recovery, tenant isolation, outcome bridge and generation mirror are durable', { skip: process.env.PAIGE_SOCIAL_TEST_POSTGRES !== 'true' }, async () => {
  const { startDisposablePostgres } = require('./helpers/disposablePostgres');
  pgInstance = await startDisposablePostgres('paige-social-pg-');
  pgPool = new Pool({ connectionString: pgInstance.connectionString });
  pgPool.on('error', onPoolError);
  const store = createPostgresSocialContentStore(pgPool); await store.ensureSchema();
  await pgPool.query(fs.readFileSync(require.resolve('../migrations/2026-09-24-paige-governed-social.sql'), 'utf8'));
  await pgPool.query(fs.readFileSync(require.resolve('../migrations/2026-08-13-content-outcome-intelligence.sql'), 'utf8'));
  await pgPool.query("CREATE TABLE pending_comments(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, status TEXT, posted_at TIMESTAMPTZ, author_name TEXT, author_title TEXT, post_content TEXT, comment TEXT, channel TEXT)");
  const pendingId = (await pgPool.query("INSERT INTO pending_comments(client_id,status) VALUES(10,'pending') RETURNING id")).rows[0].id;
  let release; const gate = new Promise(r => { release = r; });
  registerDisposable(() => { release(); });
  const f = await fixture({ store, pendingCommentId: pendingId, send: async () => { await gate; return { success: true, externalPostId: 'pg-post' }; },
    syncOutcome: a => trackPromise(require('../services/paigeSocialOutcome').syncPaigeSocialOutcome(pgPool, a)) });
  publisher = f.cap;
  receiptRecovery = f.cap;
  await f.approve();
  assert.equal(await store.getById(f.artifact.id, '11', 11), null);
  const first = trackPromise(f.publish()); while (!f.counts().sends) await new Promise(r => setImmediate(r));
  const second = await trackPromise(f.publish()); assert.equal(second.status, 'failed'); release(); assert.equal((await first).status, 'completed');
  assert.equal(f.counts().sends, 1);
  const row = await createPostgresSocialContentStore(pgPool).getById(f.artifact.id, '10', 10);
  assert.equal(row.publication.providerPostId, 'pg-post'); assert.equal(row.publication.outcomeId, f.artifact.id);
  await trackPromise(f.publish());
  assert.equal((await pgPool.query('SELECT count(*)::int AS n FROM content_publications')).rows[0].n, 1);
  assert.equal((await pgPool.query('SELECT status FROM pending_comments WHERE id=$1', [pendingId])).rows[0].status, 'posted');
  const outcomes = require('../services/contentOutcomeIntelligence'); const outcomeStore = outcomes.createPostgresStore(pgPool);
  outcomeBridge = outcomeStore;
  await outcomes.addPerformanceSnapshot(row.publication.outcomeId, { clientId: 10, impressions: 400, comments: 3 }, { store: outcomeStore });
  const full = await outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 10, store: outcomeStore });
  assert.equal(full.performanceSnapshots.length, 1);
  await assert.rejects(outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 11, store: outcomeStore }), /not found for tenant/);
  const { createSocialContentCapability } = require('../packages/capabilities/contentGeneration/SocialContent');
  const cap = createSocialContentCapability({ pool: pgPool, getLearnings: async () => [], runGeneration: async () => ({ success: true, drafts: [{ company: { name: 'Anchor' }, channel: 'linkedin_page', content: 'Manchester offices can request a facilities assessment.', contentType: 'educational' }] }) });
  generationMirror = cap;
  const generated = await trackPromise(cap.execute({ tenantId: '10', clientId: 10, inputs: { platform: 'linkedin_page', contentObjective: 'awareness', campaignId: 'campaign-2' } }));
  assert.equal(generated.status, 'completed'); assert.ok(generated.outputs.artifacts[0].pendingCommentId);
  const mirrored = (await pgPool.query('SELECT * FROM pending_comments WHERE id=$1', [generated.outputs.artifacts[0].pendingCommentId])).rows[0];
  assert.equal(mirrored.comment, generated.outputs.artifacts[0].body); assert.doesNotMatch(mirrored.comment, /FIRST_COMMENT|gopulseforge/);
  assert.equal(mirrored.client_id, 10);
});

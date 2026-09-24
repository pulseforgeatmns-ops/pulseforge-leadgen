'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { Pool } = require('pg');
const { createPostgresSocialContentStore } = require('../packages/capabilities/contentGeneration/SocialContentStore');
const { fixture } = require('./helpers/paigeSocialFixture');

test('PostgreSQL locks, receipt recovery, tenant isolation, outcome bridge and generation mirror are durable', { skip: process.env.PAIGE_SOCIAL_TEST_POSTGRES !== 'true' }, async () => {
  const { startDisposablePostgres } = require('./helpers/disposablePostgres');
  const instance = await startDisposablePostgres('paige-social-pg-');
  const pool = new Pool({ connectionString: instance.connectionString });
  try {
    const store = createPostgresSocialContentStore(pool); await store.ensureSchema();
    await pool.query(fs.readFileSync(require.resolve('../migrations/2026-09-24-paige-governed-social.sql'), 'utf8'));
    await pool.query(fs.readFileSync(require.resolve('../migrations/2026-08-13-content-outcome-intelligence.sql'), 'utf8'));
    await pool.query("CREATE TABLE pending_comments(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, status TEXT, posted_at TIMESTAMPTZ, author_name TEXT, author_title TEXT, post_content TEXT, comment TEXT, channel TEXT)");
    const pendingId = (await pool.query("INSERT INTO pending_comments(client_id,status) VALUES(10,'pending') RETURNING id")).rows[0].id;
    let release; const gate = new Promise(r => { release = r; });
    const f = await fixture({ store, pendingCommentId: pendingId, send: async () => { await gate; return { success: true, externalPostId: 'pg-post' }; },
      syncOutcome: a => require('../services/paigeSocialOutcome').syncPaigeSocialOutcome(pool, a) });
    await f.approve();
    assert.equal(await store.getById(f.artifact.id, '11', 11), null);
    const first = f.publish(); while (!f.counts().sends) await new Promise(r => setImmediate(r));
    const second = await f.publish(); assert.equal(second.status, 'failed'); release(); assert.equal((await first).status, 'completed');
    assert.equal(f.counts().sends, 1);
    const row = await createPostgresSocialContentStore(pool).getById(f.artifact.id, '10', 10);
    assert.equal(row.publication.providerPostId, 'pg-post'); assert.equal(row.publication.outcomeId, f.artifact.id);
    await f.publish();
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM content_publications')).rows[0].n, 1);
    assert.equal((await pool.query('SELECT status FROM pending_comments WHERE id=$1', [pendingId])).rows[0].status, 'posted');
    const outcomes = require('../services/contentOutcomeIntelligence'); const outcomeStore = outcomes.createPostgresStore(pool);
    await outcomes.addPerformanceSnapshot(row.publication.outcomeId, { clientId: 10, impressions: 400, comments: 3 }, { store: outcomeStore });
    const full = await outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 10, store: outcomeStore });
    assert.equal(full.performanceSnapshots.length, 1);
    await assert.rejects(outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 11, store: outcomeStore }), /not found for tenant/);
    const { createSocialContentCapability } = require('../packages/capabilities/contentGeneration/SocialContent');
    const cap = createSocialContentCapability({ pool, getLearnings: async () => [], runGeneration: async () => ({ success: true, drafts: [{ company: { name: 'Anchor' }, channel: 'linkedin_page', content: 'Manchester offices can request a facilities assessment.', contentType: 'educational' }] }) });
    const generated = await cap.execute({ tenantId: '10', clientId: 10, inputs: { platform: 'linkedin_page', contentObjective: 'awareness', campaignId: 'campaign-2' } });
    assert.equal(generated.status, 'completed'); assert.ok(generated.outputs.artifacts[0].pendingCommentId);
    const mirrored = (await pool.query('SELECT * FROM pending_comments WHERE id=$1', [generated.outputs.artifacts[0].pendingCommentId])).rows[0];
    assert.equal(mirrored.comment, generated.outputs.artifacts[0].body); assert.doesNotMatch(mirrored.comment, /FIRST_COMMENT|gopulseforge/);
    assert.equal(mirrored.client_id, 10);
  } finally { await pool.end(); await instance.stop(); }
});

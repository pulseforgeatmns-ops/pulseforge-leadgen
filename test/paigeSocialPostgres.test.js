'use strict';

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const { createTrackedPostgresPool, closePostgresFixture } = require('./helpers/closePostgresPool');
const { createPostgresSocialContentStore } = require('../packages/capabilities/contentGeneration/SocialContentStore');
const { fixture } = require('./helpers/paigeSocialFixture');

const enabled = process.env.PAIGE_SOCIAL_TEST_POSTGRES === 'true';
const dualWritePath = require.resolve('../utils/knowledgeDualWrite');

(enabled ? describe : describe.skip)('Paige social PostgreSQL durability', () => {
  const inFlight = new Set();

  /** @param {Promise<unknown>} promise */
  function track(promise) {
    if (!promise || typeof promise.then !== 'function') return promise;
    inFlight.add(promise);
    promise
      .catch(() => {})
      .finally(() => {
        inFlight.delete(promise);
      });
    return promise;
  }

  /** @type {import('pg').Pool | null} */
  let pool = null;
  /** @type {{ stop?: () => Promise<void> } | null} */
  let instance = null;
  /** @type {import('node:module').Module | undefined} */
  let originalDualWrite;
  /** @type {string | undefined} */
  let previousDualWriteFlag;

  before(() => {
    originalDualWrite = require.cache[dualWritePath];
    require.cache[dualWritePath] = {
      id: dualWritePath,
      filename: dualWritePath,
      loaded: true,
      exports: { safeWriteOperational() {}, OPERATIONAL_EVENTS: {} },
    };
    previousDualWriteFlag = process.env.KNOWLEDGE_DUAL_WRITE;
    process.env.KNOWLEDGE_DUAL_WRITE = '0';
  });

  after(async () => {
    await Promise.allSettled([...inFlight]);

    try {
      // Pool.end() can resolve before physical sockets close. The shared test
      // helper waits for client end events before stopping PostgreSQL and keeps
      // query, pool, socket-close and server-stop failures visible.
      if (pool) await closePostgresFixture(pool, instance);
      else if (instance?.stop) await instance.stop();
    } finally {
      instance = null;
      pool = null;
      if (originalDualWrite) require.cache[dualWritePath] = originalDualWrite;
      else delete require.cache[dualWritePath];
      if (previousDualWriteFlag === undefined) delete process.env.KNOWLEDGE_DUAL_WRITE;
      else process.env.KNOWLEDGE_DUAL_WRITE = previousDualWriteFlag;
    }
  });

  it('PostgreSQL locks, receipt recovery, tenant isolation, outcome bridge and generation mirror are durable', async () => {
    const { startDisposablePostgres } = require('./helpers/disposablePostgres');
    instance = await startDisposablePostgres('paige-social-pg-');

    pool = createTrackedPostgresPool({ connectionString: instance.connectionString });

    const store = createPostgresSocialContentStore(pool);
    await store.ensureSchema();
    await pool.query(fs.readFileSync(require.resolve('../migrations/2026-09-24-paige-governed-social.sql'), 'utf8'));
    await pool.query(fs.readFileSync(require.resolve('../migrations/2026-08-13-content-outcome-intelligence.sql'), 'utf8'));
    await pool.query(
      "CREATE TABLE pending_comments(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), client_id INTEGER, status TEXT, posted_at TIMESTAMPTZ, author_name TEXT, author_title TEXT, post_content TEXT, comment TEXT, channel TEXT)"
    );
    const pendingId = (await pool.query("INSERT INTO pending_comments(client_id,status) VALUES(10,'pending') RETURNING id")).rows[0].id;

    let release;
    const gate = new Promise((r) => {
      release = r;
    });
    const f = await fixture({
      store,
      pendingCommentId: pendingId,
      send: async () => {
        await gate;
        return { success: true, externalPostId: 'pg-post' };
      },
      syncOutcome: (artifact) =>
        track(require('../services/paigeSocialOutcome').syncPaigeSocialOutcome(pool, artifact)),
    });
    const publish = (inputs) => track(f.publish(inputs));

    await f.approve();
    assert.equal(await store.getById(f.artifact.id, '11', 11), null);

    const first = publish();
    while (!f.counts().sends) await new Promise((r) => setImmediate(r));
    const second = await publish();
    assert.equal(second.status, 'failed');
    release();
    assert.equal((await first).status, 'completed');
    assert.equal(f.counts().sends, 1);

    const row = await createPostgresSocialContentStore(pool).getById(f.artifact.id, '10', 10);
    assert.equal(row.publication.providerPostId, 'pg-post');
    assert.equal(row.publication.outcomeId, f.artifact.id);

    await publish();
    assert.equal((await pool.query('SELECT count(*)::int AS n FROM content_publications')).rows[0].n, 1);
    assert.equal((await pool.query('SELECT status FROM pending_comments WHERE id=$1', [pendingId])).rows[0].status, 'posted');

    const outcomes = require('../services/contentOutcomeIntelligence');
    const outcomeStore = outcomes.createPostgresStore(pool);
    await outcomes.addPerformanceSnapshot(
      row.publication.outcomeId,
      { clientId: 10, impressions: 400, comments: 3 },
      { store: outcomeStore }
    );
    const full = await outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 10, store: outcomeStore });
    assert.equal(full.performanceSnapshots.length, 1);
    await assert.rejects(
      outcomes.getPublicationOutcome(row.publication.outcomeId, { clientId: 11, store: outcomeStore }),
      /not found for tenant/
    );

    const { createSocialContentCapability } = require('../packages/capabilities/contentGeneration/SocialContent');
    const cap = createSocialContentCapability({
      pool,
      getLearnings: async () => [],
      runGeneration: async () => ({
        success: true,
        drafts: [{
          company: { name: 'Anchor' },
          channel: 'linkedin_page',
          content: 'Manchester offices can request a facilities assessment.',
          contentType: 'educational',
        }],
      }),
    });
    const generated = await track(
      cap.execute({
        tenantId: '10',
        clientId: 10,
        inputs: {
          platform: 'linkedin_page',
          contentObjective: 'awareness',
          campaignId: 'campaign-2',
        },
      })
    );
    assert.equal(generated.status, 'completed');
    assert.ok(generated.outputs.artifacts[0].pendingCommentId);
    const mirrored = (await pool.query('SELECT * FROM pending_comments WHERE id=$1', [generated.outputs.artifacts[0].pendingCommentId])).rows[0];
    assert.equal(mirrored.comment, generated.outputs.artifacts[0].body);
    assert.doesNotMatch(mirrored.comment, /FIRST_COMMENT|gopulseforge/);
    assert.equal(mirrored.client_id, 10);

    await Promise.allSettled([...inFlight]);
  });
});
